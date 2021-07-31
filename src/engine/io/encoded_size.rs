/// Provides the expected size of the serialized form so repeated serialization
/// is not needed to find space.

pub trait ConstEncodedSize {
    fn encoded_size() -> usize;
}

pub trait SelfEncodedSize {
    fn encoded_size(&self) -> usize;
}
pub trait EncodedSize<T> {
    fn encoded_size(input: T) -> usize;
}
