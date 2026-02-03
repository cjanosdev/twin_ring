use scylla::{Session, SessionBuilder};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let session: Session = SessionBuilder::new()
        .known_node("cassandra:9042")
        .build()
        .await?;

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS kvstore WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS kvstore.kv (key text PRIMARY KEY, value text)",
            &[],
        )
        .await?;

    for i in 0..100_000 {
        let key = format!("key{}", i);
        let val = format!("val{}", i);
        session
            .query("INSERT INTO kvstore.kv (key, value) VALUES (?, ?)", (key, val))
            .await?;
    }

    println!("✅ Preloaded 100k keys into Cassandra");
    Ok(())
}








// use cassandra_cpp::{Cluster, Error};
// use anyhow::Result;

// #[tokio::main]
// async fn main() -> Result<(), Error> {
//     // Step 1: create the cluster object
//     let mut cluster = Cluster::default();

//     // Step 2: configure it
//     cluster.set_contact_points("cassandra")?; // service name in docker-compose
//     cluster.set_port(9042)?;

//     // Step 3: connect
//     let session = cluster.connect().await?;

//     // Make sure keyspace and table exist
//     let create_keyspace = "
//         CREATE KEYSPACE IF NOT EXISTS kvstore
//         WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
//     ";
    
//     session.execute(create_keyspace).await?;
    
//     let create_table = "
//     CREATE TABLE IF NOT EXISTS kvstore.kv (
//         key text PRIMARY KEY,
//         value text
//         );
//         ";

//     session.execute(create_table).await?;
    
//     println!("✅ Keyspace and table ready");

//     // Insert a bunch of records
//     for i in 0..100_000 {
//         let key = format!("key{}", i);
//         let val = format!("val{}", i);

//         let mut statement = session.statement("INSERT INTO kvstore.kv (key, value) VALUES (?, ?)");
//         statement.bind_string(0, &key)?;
//         statement.bind_string(1, &val)?;
//         statement.execute().await?;
//     }

//     println!("✅ Preloaded 100k keys into Cassandra");
//     Ok(())
// }
