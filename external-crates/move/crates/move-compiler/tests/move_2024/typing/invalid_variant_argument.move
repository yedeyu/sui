module 0x42::m {

    public enum E {
        A(u64)
    }

    public enum ET<T> {
        A(T,T)
    }

    fun t0(): E {
        E::A(true)
    }

    fun t1(): EA<u64> {
        ET::A(0,true)
    }

}
