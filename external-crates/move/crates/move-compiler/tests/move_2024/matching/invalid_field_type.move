module 0x42::m {

    public enum A {
        A(u64)
    }

    public enum B<T> {
        B(T,T)
    }

    fun t0(a: &A, default: &u64): &u64 {
        match (a) {
            A::A(false) => default,
            A::A(n) => n
        }
    }

    fun t1(a: &B<u64>) {
        match (a) {
            B::B(false, _) => (),
            B::B(_, _) => (),
        }
    }

}
