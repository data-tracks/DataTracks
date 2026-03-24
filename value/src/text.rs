use crate::value_display;
use crate::{Bool, Float, Int};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use speedy::{Context, Readable, Reader, Writable, Writer};
use std::cmp::PartialEq;
use std::fmt::Formatter;

#[derive(Eq, Hash, Debug, PartialEq, Clone, Serialize, Deserialize, Ord, PartialOrd)]
#[repr(transparent)]
pub struct Text(pub SmolStr);

impl<'a, C: Context> Readable<'a, C> for Text {
    #[inline]
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let s: String = reader.read_value()?;
        Ok(Text(SmolStr::new(s)))
    }
}

impl<'a, C: Context> Writable<C> for Text {
    #[inline]
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        let s = self.0.to_string();
        writer.write_value(&s)
    }

    #[inline]
    fn bytes_needed(&self) -> Result<usize, C::Error> {
        Ok(4 + self.0.len())
    }
}

impl std::ops::Deref for Text {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Text {
    fn from(s: String) -> Self {
        Self(SmolStr::new(s))
    }
}

impl From<&str> for Text {
    fn from(s: &str) -> Self {
        Self(SmolStr::new(s))
    }
}

impl PartialEq<Int> for Text {
    fn eq(&self, other: &Int) -> bool {
        other == self
    }
}

impl PartialEq<Float> for Text {
    fn eq(&self, other: &Float) -> bool {
        other == self
    }
}

impl PartialEq<Bool> for Text {
    fn eq(&self, other: &Bool) -> bool {
        other == self
    }
}

value_display!(Text);
