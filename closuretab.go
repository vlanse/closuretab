package closuretab

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type AttrType int

const (
	Child AttrType = iota
	Parent
	Depth
)

type AttrMapping = map[AttrType]string

type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type Node struct {
	ID       int64
	ParentID int64
	Depth    int
}

type ClosureRelation struct {
	table string
	attrs map[AttrType]string
}

func InitClosureRelation(tableName string, attrs AttrMapping) *ClosureRelation {
	return &ClosureRelation{table: tableName, attrs: attrs}
}

func (r *ClosureRelation) GetChildren(ctx context.Context, q Querier, parentID int64) ([]Node, error) {
	rows, err := q.QueryContext(
		ctx,
		fmt.Sprintf(
			"SELECT %s, %s, %s FROM %s WHERE %s = ? ORDER BY %s ASC",
			r.attrs[Child], r.attrs[Parent], r.attrs[Depth], r.table,
			r.attrs[Parent], r.attrs[Depth],
		),
		parentID,
	)
	if err != nil {
		return nil, fmt.Errorf("get child nodes for parent ID %d: %w", parentID, err)
	}
	return scanNodes(rows)
}

func (r *ClosureRelation) GetParents(ctx context.Context, q Querier, nodeID int64) ([]Node, error) {
	rows, err := q.QueryContext(
		ctx,
		fmt.Sprintf(
			"SELECT %s, %s, %s FROM %s WHERE %s = ? AND %s != ? ORDER BY %s DESC",
			r.attrs[Parent], r.attrs[Parent], r.attrs[Depth], r.table,
			r.attrs[Child], r.attrs[Parent],
			r.attrs[Depth],
		),
		nodeID, nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("get parent nodes for node ID %d: %w", nodeID, err)
	}
	return scanNodes(rows)
}

func (r *ClosureRelation) Insert(ctx context.Context, q Querier, parentID, nodeID int64) (Node, error) {
	_, err := q.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s) "+
				"SELECT ?, %s, %s + 1 FROM %s WHERE %s = ?",
			r.table, r.attrs[Child], r.attrs[Parent], r.attrs[Depth],
			r.attrs[Parent], r.attrs[Depth], r.table, r.attrs[Child],
		),
		nodeID, parentID,
	)
	if err != nil {
		return Node{}, fmt.Errorf("insert hierarchy references: %w", err)
	}

	_, err = q.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
			r.table, r.attrs[Child], r.attrs[Parent], r.attrs[Depth],
		),
		nodeID, nodeID, 0,
	)
	if err != nil {
		return Node{}, fmt.Errorf("insert self-reference: %w", err)
	}

	return Node{}, nil
}

func (r *ClosureRelation) Delete(ctx context.Context, q Querier, nodeID int64) error {
	if _, err := q.ExecContext(
		ctx,
		fmt.Sprintf(
			"DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s = ?)",
			r.table, r.attrs[Child], r.attrs[Child], r.table, r.attrs[Parent],
		),
		nodeID,
	); err != nil {
		return fmt.Errorf("remove node ID %d: %w", nodeID, err)
	}

	if _, err := q.ExecContext(
		ctx,
		fmt.Sprintf(
			"DELETE FROM %s WHERE %s = ? OR %s = ?",
			r.table, r.attrs[Child], r.attrs[Parent],
		),
		nodeID, nodeID,
	); err != nil {
		return fmt.Errorf("remove node ID %d: %w", nodeID, err)
	}
	return nil
}

func (r *ClosureRelation) Move(ctx context.Context, q Querier, nodeID, newParentID int64) error {
	if _, deleteErr := q.ExecContext(
		ctx,
		fmt.Sprintf(
			"DELETE FROM %s "+
				"WHERE %s IN "+
				"(SELECT %s FROM %s WHERE %s = ?) "+
				"AND %s IN "+
				"(SELECT %s FROM %s WHERE %s = ? AND %s != %s) ",
			r.table,
			r.attrs[Child],
			r.attrs[Child], r.table, r.attrs[Parent],
			r.attrs[Parent],
			r.attrs[Parent], r.table, r.attrs[Child], r.attrs[Parent], r.attrs[Child],
		),
		nodeID, nodeID,
	); deleteErr != nil {
		return fmt.Errorf("remove node ID %d: %w", nodeID, deleteErr)
	}

	parents, err := r.GetParents(ctx, q, newParentID)
	if err != nil {
		return fmt.Errorf("get new parents for moved nodes: %w", err)
	}
	parentIDs := NodeIDs(parents)
	parentIDs = append(parentIDs, newParentID)
	parentIDsPlaceholders := makePlaceholders("?", len(parentIDs))

	children, err := r.GetChildren(ctx, q, nodeID)
	if err != nil {
		return fmt.Errorf("get all nodes being moved: %w", err)
	}
	childrenIDs := NodeIDs(children)
	childrenIDsPlaceholders := makePlaceholders("?", len(childrenIDs))

	args := make([]interface{}, len(parentIDs)+len(childrenIDs))
	for i := 0; i < len(args); i++ {
		if i < len(parentIDs) {
			args[i] = parentIDs[i]
		} else {
			args[i] = childrenIDs[i-len(parentIDs)]
		}
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s, %s, %s)
        	SELECT supertree.%s, subtree.%s, MAX(supertree.%s + subtree.%s + 1)
        	FROM %s AS supertree, %s AS subtree
        	WHERE 
        	    supertree.%s IN %s
        		AND subtree.%s IN %s
			GROUP BY supertree.%s, subtree.%s`,
		r.table, r.attrs[Parent], r.attrs[Child], r.attrs[Depth],
		r.attrs[Parent], r.attrs[Child], r.attrs[Depth], r.attrs[Depth],
		r.table, r.table,
		r.attrs[Parent], parentIDsPlaceholders,
		r.attrs[Child], childrenIDsPlaceholders,
		r.attrs[Parent], r.attrs[Child],
	)

	if _, insertErr := q.ExecContext(
		ctx,
		query,
		args...,
	); insertErr != nil {
		return fmt.Errorf("insert nodes under new parent: %w", insertErr)
	}

	return nil
}

func (r *ClosureRelation) Empty(ctx context.Context, q Querier) (bool, error) {
	row := q.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", r.table))
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		return false, fmt.Errorf("count closure table rows: %w", err)
	}
	return cnt == 0, nil
}

func NodeIDs(nodes []Node) []int64 {
	res := make([]int64, 0, len(nodes))
	for i := range nodes {
		res = append(res, nodes[i].ID)
	}
	return res
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanNodes(rows *sql.Rows) ([]Node, error) {
	result := make([]Node, 0)
	if _, scanErr := scanEachRow(rows, func(s scanner) error {
		n := Node{}
		if rowErr := s.Scan(&n.ID, &n.ParentID, &n.Depth); rowErr != nil {
			return rowErr
		}
		result = append(result, n)
		return nil
	}); scanErr != nil {
		return nil, fmt.Errorf("scan nodes: %w", scanErr)
	}
	return result, nil
}

func scanEachRow(rows *sql.Rows, scanRow func(s scanner) error) (rowsProcessed int, err error) {
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		err = scanRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan row: %w", err)
		}
		count++
	}
	if err = rows.Err(); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("rows scan: %w", err)
	}
	return count, nil
}

func makePlaceholders(pHolder string, argLen int) string {
	pHolders := make([]string, argLen)
	for i := 0; i < argLen; i++ {
		pHolders[i] = pHolder
	}
	return fmt.Sprintf("(%s)", strings.Join(pHolders, ", "))
}
