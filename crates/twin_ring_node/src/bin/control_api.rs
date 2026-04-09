use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use bollard::Docker;
use bollard::container::{StopContainerOptions, StartContainerOptions, StatsOptions};
use futures_util::StreamExt;

#[actix_web::post("/kill/{node}")]
async fn kill_node(path: web::Path<String>) -> impl Responder {
    let node = path.into_inner();
    let name = format!("docker-cache_node_{}-1", node);
    println!("🧨 Stopping {}", name);

    let docker = Docker::connect_with_unix_defaults().unwrap();
    match docker.stop_container(&name, Some(StopContainerOptions { t: 1 })).await {
        Ok(_) => HttpResponse::Ok().body(format!("✅ Stopped {}", name)),
        Err(e) => HttpResponse::InternalServerError().body(format!("❌ {e}")),
    }
}

#[actix_web::post("/start/{node}")]
async fn start_node(path: web::Path<String>) -> impl Responder {
    let node = path.into_inner();
    let name = format!("docker-cache_node_{}-1", node);
    println!("🚀 Starting {}", name);

    let docker = Docker::connect_with_unix_defaults().unwrap();
    match docker.start_container::<String>(&name, None::<StartContainerOptions<String>>).await {
        Ok(_) => HttpResponse::Ok().body(format!("✅ Started {}", name)),
        Err(e) => HttpResponse::InternalServerError().body(format!("❌ {e}")),
    }
}

/// Returns Cassandra container memory usage as JSON:
/// { "mem_pct": f64, "used_bytes": u64, "limit_bytes": u64 }
#[actix_web::get("/cassandra-mem")]
async fn cassandra_mem() -> impl Responder {
    let docker = Docker::connect_with_unix_defaults().unwrap();
    let mut stream = docker.stats("cassandra", Some(StatsOptions { stream: false, one_shot: true }));
    match stream.next().await {
        Some(Ok(stats)) => {
            let used  = stats.memory_stats.usage.unwrap_or(0);
            let limit = stats.memory_stats.limit.unwrap_or(1);
            let pct   = used as f64 / limit as f64 * 100.0;
            HttpResponse::Ok().json(serde_json::json!({
                "mem_pct":     pct,
                "used_bytes":  used,
                "limit_bytes": limit,
            }))
        }
        _ => HttpResponse::InternalServerError().body("cassandra stats unavailable"),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("🎮 Control API running on port 9000");
    HttpServer::new(|| {
        App::new()
            .service(kill_node)
            .service(start_node)
            .service(cassandra_mem)
    })
    .bind(("0.0.0.0", 9000))?
    .run()
    .await
}
