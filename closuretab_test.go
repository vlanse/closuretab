package closuretab

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/vlanse/dbmigrate"
	_ "modernc.org/sqlite"
)

type closureSuite struct {
	suite.Suite
	db *sql.DB
}

func TestClosure(t *testing.T) {
	suite.Run(t, &closureSuite{})
}

func (s *closureSuite) TestInsert() {
	cl := initClosure()
	ctx := context.Background()

	root, err := cl.Insert(ctx, s.db, 0, 0)
	s.Require().NoError(err)

	_, err = cl.Insert(ctx, s.db, root.ID, 1)
	s.Require().NoError(err)

	children, err := cl.GetChildren(ctx, s.db, root.ID)
	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]Node{{ID: 0, ParentID: 0, Depth: 0}, {ID: 1, ParentID: 0, Depth: 1}},
		children,
	)

	parents, err := cl.GetParents(ctx, s.db, 1)
	s.Require().NoError(err)
	s.Require().ElementsMatch([]Node{{ID: 0, ParentID: 0, Depth: 1}}, parents)
}

func (s *closureSuite) TestEmpty() {
	cl := initClosure()
	ctx := context.Background()

	empty, err := cl.Empty(ctx, s.db)
	s.Require().NoError(err)
	s.Require().True(empty)

	_, err = cl.Insert(ctx, s.db, 1, 1)
	s.Require().NoError(err)

	empty, err = cl.Empty(ctx, s.db)
	s.Require().NoError(err)
	s.Require().False(empty)
}

func (s *closureSuite) TestDelete() {
	cl := initClosure()
	ctx := context.Background()

	_, err := cl.Insert(ctx, s.db, 0, 0)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 0, 1)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 1, 2)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 2, 3)
	s.Require().NoError(err)
	ch, err := cl.GetChildren(ctx, s.db, 0)

	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]Node{
			{ID: 0, ParentID: 0, Depth: 0},
			{ID: 1, ParentID: 0, Depth: 1},
			{ID: 2, ParentID: 0, Depth: 2},
			{ID: 3, ParentID: 0, Depth: 3},
		},
		ch,
	)

	s.Require().NoError(cl.Delete(ctx, s.db, 2))

	ch, err = cl.GetChildren(ctx, s.db, 0)
	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]Node{{ID: 0, ParentID: 0, Depth: 0}, {ID: 1, ParentID: 0, Depth: 1}},
		ch,
	)

	s.Require().NoError(cl.Delete(ctx, s.db, 0))
	empty, err := cl.Empty(ctx, s.db)
	s.Require().NoError(err)
	s.Require().True(empty)
}

func (s *closureSuite) TestMove() {
	cl := initClosure()
	ctx := context.Background()

	_, err := cl.Insert(ctx, s.db, 0, 0)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 0, 1)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 1, 2)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 2, 3)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 2, 4)
	s.Require().NoError(err)
	_, err = cl.Insert(ctx, s.db, 0, 5)
	s.Require().NoError(err)

	ch, err := cl.GetChildren(ctx, s.db, 0)
	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]Node{
			{ID: 0, ParentID: 0, Depth: 0},
			{ID: 1, ParentID: 0, Depth: 1},
			{ID: 2, ParentID: 0, Depth: 2},
			{ID: 3, ParentID: 0, Depth: 3},
			{ID: 4, ParentID: 0, Depth: 3},
			{ID: 5, ParentID: 0, Depth: 1},
		},
		ch,
	)

	s.Require().NoError(cl.Move(ctx, s.db, 2, 5))
	ch, err = cl.GetChildren(ctx, s.db, 0)
	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]Node{
			{ID: 0, ParentID: 0, Depth: 0},
			{ID: 1, ParentID: 0, Depth: 1},
			{ID: 2, ParentID: 0, Depth: 2},
			{ID: 3, ParentID: 0, Depth: 3},
			{ID: 4, ParentID: 0, Depth: 3},
			{ID: 5, ParentID: 0, Depth: 1},
		},
		ch,
	)
}

func (s *closureSuite) SetupTest() {
	db, err := sql.Open("sqlite", ":memory:")
	s.Require().NoError(err)

	s.db = db

	mm := []dbmigrate.Migration{
		{
			ID:   "1",
			Desc: "initial",
			Stmt: `
				CREATE TABLE closure
				(
					id INTEGER,
					parent_id INTEGER,
					depth INTEGER
				);
				`,
		},
	}
	s.Require().NoError(dbmigrate.UpgradeToLatest(s.db, dbmigrate.DialectSQLite, mm...))
}

func initClosure() *ClosureRelation {
	return InitClosureRelation(
		"closure",
		AttrMapping{
			Child:  "id",
			Parent: "parent_id",
			Depth:  "depth",
		},
	)
}
