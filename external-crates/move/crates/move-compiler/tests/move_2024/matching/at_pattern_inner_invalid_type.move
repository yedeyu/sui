module 0x42::m {

    public enum Maybe<T> {
        Just(T),
        Nothing
    }

    fun t0<T>(m: Maybe<T>) {
        match (m) {
            x @ 0 => (),
            Maybe::Nothing => ()
        }
    }

    fun t1<T>(m: Maybe<T>) {
        match (m) {
            _x @ 0 => (),
            Maybe::Nothing => ()
        }
    }


}
