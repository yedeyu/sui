module 0x42::m {

    public enum A {
        A(u64)
    }

    public enum B<T,U> {
        B(T,U)
    }

    fun t0(a: &A, default: &u64): &u64 {
        match (a) {
            A::A(_) => default,
            A::A(_n) => (),
        }
    }

    fun t1<T,U>(a: &B<T,U>): &T {
        match (a) {
            B::B(m, _) => m,
            B::B(_, m) => m
        }
    }

    fun t2(a: &B<bool, u64>) {
        let _ = match (a) {
            B::B(m, _) if m == &6 => m,
            B::B(m, _) => m,
            B::B(_, n) => n
        };
    }

}
