//Trying to figure out how to store a dynamic list of elements (columns of a row) and store it
// Stackoverflow hint https://stackoverflow.com/q/27957103/160208

//We should start with a naive implementation, we're just going to support pg_attribute as it is right now
    //id: uuid, not null
    //name: string, not null
    //parent: uuid, not null

//Mandatory min items for a row
    //transaction ID for insert
    //transaction ID for delete
    //data from above
        //uuid (fixed length)
        //string (variable length)
        //parent (fixed length) 

struct Row {}

trait PgType {
    fn get
}

Vec<Box<dyn ThingTrait + 'a>>