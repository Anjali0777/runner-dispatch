package dispatch_test

import (
	"fmt"

	"github.com/Anjali0777/runner-dispatch/dispatch"
)

func Example() {
	labels := dispatch.NewInterner()
	d := dispatch.NewLocked(dispatch.NewIndexed(dispatch.Caps{
		Default: 20,
		PerOrg:  map[string]int{"acme": 1}, // acme may run one job at a time
	}))

	d.Enqueue(dispatch.Job{ID: 1, Org: "acme", Labels: labels.MustMask("ubuntu-latest")})
	d.Enqueue(dispatch.Job{ID: 2, Org: "acme", Labels: labels.MustMask("ubuntu-latest")})
	d.Enqueue(dispatch.Job{ID: 3, Org: "globex", Labels: labels.MustMask("ubuntu-latest", "gpu")})
	d.Enqueue(dispatch.Job{ID: 4, Org: "initech", Labels: labels.MustMask("ubuntu-latest")})

	plain := labels.MustMask("ubuntu-latest")
	gpu := labels.MustMask("ubuntu-latest", "gpu")

	for _, r := range []dispatch.Mask{plain, plain, gpu, plain} {
		if j, ok := d.Assign(r); ok {
			fmt.Printf("runner %v -> job %d (%s)\n", labels.Names(r), j.ID, j.Org)
		} else {
			fmt.Printf("runner %v -> nothing eligible\n", labels.Names(r))
		}
	}
	d.Complete(1) // acme's first job finishes, freeing its slot
	j, _ := d.Assign(plain)
	fmt.Printf("after completion -> job %d (%s)\n", j.ID, j.Org)

	// Output:
	// runner [ubuntu-latest] -> job 1 (acme)
	// runner [ubuntu-latest] -> job 4 (initech)
	// runner [gpu ubuntu-latest] -> job 3 (globex)
	// runner [ubuntu-latest] -> nothing eligible
	// after completion -> job 2 (acme)
}
