use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use bollard::container::{KillContainerOptions, StartContainerOptions, StatsOptions};
use bollard::Docker;
use futures_util::StreamExt;

#[actix_web::post("/kill/{node}")]
async fn kill_node(path: web::Path<String>) -> impl Responder {
    let Some(name) = cache_container_name(&path.into_inner()) else {
        return HttpResponse::BadRequest().body("node must be 1, 2, or 3");
    };
    println!("🧨 Stopping {}", name);

    let docker = match Docker::connect_with_unix_defaults() {
        Ok(docker) => docker,
        Err(error) => return HttpResponse::InternalServerError().body(error.to_string()),
    };
    match docker
        .kill_container(&name, None::<KillContainerOptions<String>>)
        .await
    {
        Ok(_) => HttpResponse::Ok().body(format!("✅ Killed {}", name)),
        Err(e) => HttpResponse::InternalServerError().body(format!("❌ {e}")),
    }
}

#[actix_web::post("/start/{node}")]
async fn start_node(path: web::Path<String>) -> impl Responder {
    let Some(name) = cache_container_name(&path.into_inner()) else {
        return HttpResponse::BadRequest().body("node must be 1, 2, or 3");
    };
    println!("🚀 Starting {}", name);

    let docker = match Docker::connect_with_unix_defaults() {
        Ok(docker) => docker,
        Err(error) => return HttpResponse::InternalServerError().body(error.to_string()),
    };
    match docker
        .start_container::<String>(&name, None::<StartContainerOptions<String>>)
        .await
    {
        Ok(_) => HttpResponse::Ok().body(format!("✅ Started {}", name)),
        Err(e) => HttpResponse::InternalServerError().body(format!("❌ {e}")),
    }
}

/// These names are fixed in docker-compose-baseline.yml. A closed mapping keeps
/// the control API from accepting arbitrary Docker container names from HTTP.
fn cache_container_name(node: &str) -> Option<&'static str> {
    match node {
        "1" => Some("twin_ring_cache_node_1"),
        "2" => Some("twin_ring_cache_node_2"),
        "3" => Some("twin_ring_cache_node_3"),
        _ => None,
    }
}

/// Returns Cassandra container memory usage as JSON:
/// { "mem_pct": f64, "used_bytes": u64, "limit_bytes": u64 }
#[actix_web::get("/cassandra-mem")]
async fn cassandra_mem() -> impl Responder {
    let docker = match Docker::connect_with_unix_defaults() {
        Ok(docker) => docker,
        Err(error) => return HttpResponse::InternalServerError().body(error.to_string()),
    };
    let mut stream = docker.stats(
        "cassandra",
        Some(StatsOptions {
            stream: false,
            one_shot: true,
        }),
    );
    match stream.next().await {
        Some(Ok(stats)) => {
            let used = stats.memory_stats.usage.unwrap_or(0);
            let limit = stats.memory_stats.limit.unwrap_or(1);
            let pct = used as f64 / limit as f64 * 100.0;
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
