use axum::routing::{get, post, put};
use axum::Router;
use clap::Parser;
use knowledge::agrument::KnowledgeArgument;
use knowledge::router;
use tower_http::cors::Any;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = KnowledgeArgument::parse();
    println!("args: {},{}", args.host, args.port);

    //logger init. Cannot wrap the initialization into a function, if that the logger file may not work properly!
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let file_appender = tracing_appender::rolling::daily("logs", "monitor.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    //write in json format, if not it leads unreadable characters in the log file.
    let file_layer = fmt::Layer::default().json().with_writer(non_blocking);
    let formatting_layer = fmt::layer() /*.pretty()*/
        .with_writer(std::io::stderr);
    Registry::default()
        .with(env_filter)
        // ErrorLayer 可以让 color-eyre 获取到 span 的信息
        .with(ErrorLayer::default())
        .with(formatting_layer)
        .with(file_layer)
        .init();
    color_eyre::install().unwrap();

    info!("Start Knolwdge at {:?}", std::env::current_dir().unwrap());

    //create app with routers
    let app = create_app();

    //start http server
    let http_service_url = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(http_service_url)
        .await
        .unwrap();
    info!("Listening on http://{}:{}", args.host, args.port);
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

fn create_app() -> Router {
    Router::new()
        .route("/v1", get(|| async { "Hello" }))
        .route(
            "/v1/knowledge/repository",
            put(router::load_index).post(router::create_index),
        )
        .route(
            "/v1/knowledge/query_title_body",
            post(router::find_document),
        )
        .route(
            "/v1/knowledge/query_title",
            post(router::find_document_by_title),
        )
        .route(
            "/v1/knowledge/doc",
            post(router::push_documents).delete(router::delete_document),
        )
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_methods(Any)
                // .allow_methods([
                //     Method::GET,
                //     Method::POST,
                //     Method::PUT,
                //     Method::DELETE,
                //     Method::OPTIONS,
                // ])
                .allow_headers(Any)
                .allow_origin(Any),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        )
}
