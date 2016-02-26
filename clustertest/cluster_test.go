package clustertest

import "testing"

func Test_generateJoinArg(t *testing.T) {
	if got, exp := generateJoinArg(1, 8091), "localhost:8191"; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, exp := generateJoinArg(3, 8091), "localhost:8191,localhost:8291,localhost:8391"; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}
}

func Test_shiftPort(t *testing.T) {
	got, err := shiftPort(":8080", 300)
	if err != nil {
		t.Fatal(err)
	}

	if exp := ":8380"; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, err = shiftPort("localhost:8080", 200); err != nil {
		t.Fatal(err)
	}

	if exp := "localhost:8280"; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, err = shiftPort("8080", -100); err != nil {
		t.Fatal(err)
	}

	if exp := "7980"; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, err = shiftPort("foo", -100); err == nil {
		t.Fatal("got no error, but expected one")
	}
}

func Test_ptoi(t *testing.T) {
	got, err := ptoi(":8091")
	if err != nil {
		t.Fatal(err)
	}

	if exp := 8091; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, err = ptoi("8091"); err != nil {
		t.Fatal(err)
	}

	if exp := 8091; got != exp {
		t.Fatalf("got %q, expected %q", got, exp)
	}

	if got, err = ptoi("foo"); err == nil {
		t.Fatal("got no error, but expected one")
	}
}
