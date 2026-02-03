use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use bollard::Docker;
use bollard::container::{StopContainerOptions, StartContainerOptions};

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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("🎮 Control API running on port 9000");
    HttpServer::new(|| {
        App::new()
            .service(kill_node)
            .service(start_node)
    })
    .bind(("0.0.0.0", 9000))?
    .run()
    .await
}
