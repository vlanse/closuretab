# closuretab
Closure table for RDMBS - implements hierarchical (or 'tree') data storage

```go
// init with table name (closure) and attributes:
// Child is ID of node
// Parent is ID of parent node
// Depth is depth of child relative to record's parent
cl := InitClosureRelation(
	"closure", AttrMapping{Child: "id", Parent: "parent_id", Depth: "depth"},
)

db, _ := sql.Open("sqlite", ":memory:")

// insert some items, (0, 0) is necessary root initialization
_, _ = cl.Insert(ctx, db, 0, 0)
_, _ = cl.Insert(ctx, db, 0, 1)
_, _ = cl.Insert(ctx, db, 1, 2)
_, _ = cl.Insert(ctx, db, 2, 3)

// delete subtree
_ = cl.Delete(ctx, db, 2)

_, _ = cl.Insert(ctx, db, 1, 4)
_, _ = cl.Insert(ctx, db, 4, 5)

// move subtree to another subtree
_, _ = cl.Insert(ctx, db, 0, 6)
_ = cl.Move(ctx, db, 1, 6)
```
