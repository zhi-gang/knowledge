use clap::Parser;

#[derive(Parser)]
#[command(about = "Knowledge Application")]
pub struct KnowledgeArgument{
    #[arg(long, default_value ="0.0.0.0")]
    pub host: String,

    #[arg(short, long, default_value ="4000")]  
    pub port: u16,

    #[arg(short, long,default_value = "false")]
    pub load: bool
}