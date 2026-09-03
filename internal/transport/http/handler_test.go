package httptransport

import "testing"

func TestPaginationDefaults(t *testing.T) {
	t.Parallel()

	page, pageSize := pagination(0, 0)
	if page != 1 || pageSize != 20 {
		t.Fatalf("pagination(0, 0) = (%d, %d), want (1, 20)", page, pageSize)
	}
	page, pageSize = pagination(3, 50)
	if page != 3 || pageSize != 50 {
		t.Fatalf("pagination(3, 50) = (%d, %d), want (3, 50)", page, pageSize)
	}
}
