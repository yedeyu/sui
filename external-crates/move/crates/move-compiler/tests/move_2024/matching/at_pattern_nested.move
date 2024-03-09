module 0x42::m {

    public enum Maybe<T> has copy {
        Just(T),
        Nothing
    }

    fun t<T: copy>(m: &Maybe<Maybe<T>>, t: &T): Maybe<T> {
        match (m) {
            _x @ Maybe::Just(y @ Maybe::Just(_)) => *y,
            _x @ (Maybe::Just(Maybe::Nothing) | Maybe::Nothing) => Maybe::Just(*t),
        }
    }

}
