//! The rewrite processor take a parsed query and makes it into a set of commands that can be sequentially executed.

pub enum SqlCommand {
    Select, // -> This gets very complicated
                    // Set of array inputs
                    // Join them
                    // Reduce to the data requested
    Insert, // -> This just needs a target table and values

    Update, // -> This needs target table, new values and a filter
    Delete, // -> This needs a target table and a filter
}

pub struct RewriteProcesser {}

impl RewriteProcesser {

}