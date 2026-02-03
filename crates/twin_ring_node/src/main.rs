use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use clap::Parser;
use std::time::Duration;
use twin_ring_core::Cache;
use twin_ring_core::{CsvLogger};
use num_cpus;


/// CLI args
#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 8080)]
    port: u16,
     /// Default TTL in seconds
    #[arg(long, default_value_t = 10)]
     ttl: u64,

    #[arg(long, default_value = "cache_metrics.csv")]
    log: String,
}

async fn hello() -> impl Responder {
    HttpResponse::Ok().body("TwinRing node alive!")
}

/// GET Handler for "/get/{key}"
async fn get(key: web::Path<String>, cache: web::Data<Cache>) -> impl Responder {
    if let Some(val) = cache.get(&key) {
        HttpResponse::Ok().body(val)
    } else {
        HttpResponse::NotFound().finish()
    }
}

/// PUT handler
async fn put(key: web::Path<String>, body: String, cache: web::Data<Cache>) -> impl Responder {
    cache.put(key.to_string(), body);
    HttpResponse::Ok().finish()
}

/// DELETE handler
async fn delete(key: web::Path<String>, cache: web::Data<Cache>) -> impl Responder {
    if cache.delete(&key) {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotFound().finish()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let logger = CsvLogger::new(&args.log);
    let cache = Cache::new(Duration::from_secs(args.ttl), Some(logger));

    println!("🚀 TwinRing node starting on port {} with TTL={}", args.port, args.ttl);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(cache.clone()))
            .route("/", web::get().to(hello))
            .route("/get/{key}", web::get().to(get))
            .route("/put/{key}", web::post().to(put))
            .route("/delete/{key}", web::delete().to(delete))
    })
    .workers(num_cpus::get())
    .backlog(2048)
    .max_connections(100_000)
    .max_connection_rate(25_0000)
    .keep_alive(actix_web::http::KeepAlive::Os)
    .bind(("0.0.0.0", args.port))?
    .run()
    .await
}
