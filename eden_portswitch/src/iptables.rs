use log::{info, warn};
use std::net::TcpStream;
use std::process::Command;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct RedirectRule {
    pub from_port: u16,
    pub to_port: u16,
    pub exclude_uid: Option<u32>,
    pub exclude_user: Option<String>,
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    fn as_str(&self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        }
    }
}

impl RedirectRule {
    pub fn new(from: u16, to: u16, exclude_user: Option<String>, protocol: String) -> Self {
        let protocol = match protocol.to_lowercase().as_str() {
            "udp" => Protocol::Udp,
            _ => Protocol::Tcp,
        };

        let exclude_uid = exclude_user.as_ref().and_then(|name| users::get_user_by_name(name).map(|u| u.uid()));

        Self {
            from_port: from,
            to_port: to,
            exclude_uid,
            exclude_user,
            protocol,
        }
    }
}

#[derive(Error, Debug)]
pub enum RedirectError {
    #[error("This operation requires root privileges")]
    PermissionDenied,

    #[error("Command execution failed: {0}")]
    CommandFailed(String),

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Preflight check failed: {0}")]
    PreflightFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct IptablesManager;

impl IptablesManager {
    pub fn new() -> Self {
        Self
    }

    fn run_iptables(&self, args: &[&str]) -> Result<String, RedirectError> {
        let output = Command::new("iptables").args(args).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Permission denied") || stderr.contains("Operation not permitted") {
                return Err(RedirectError::PermissionDenied);
            }
            return Err(RedirectError::CommandFailed(stderr.to_string()));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn build_rule_args(&self, rule: &RedirectRule, action: &str) -> Result<Vec<String>, RedirectError> {
        // Validate user exists if specified
        if let Some(ref username) = rule.exclude_user {
            if rule.exclude_uid.is_none() {
                return Err(RedirectError::UserNotFound(username.clone()));
            }
        }

        let mut args = vec![
            "-t".to_string(),
            "nat".to_string(),
            action.to_string(),
            "OUTPUT".to_string(),
            "-p".to_string(),
            rule.protocol.as_str().to_string(),
            "--dport".to_string(),
            rule.from_port.to_string(),
        ];

        // Add UID exclusion if specified
        if let Some(uid) = rule.exclude_uid {
            args.extend([
                "-m".to_string(),
                "owner".to_string(),
                "!".to_string(),
                "--uid-owner".to_string(),
                uid.to_string(),
            ]);
        }

        args.extend([
            "-j".to_string(),
            "REDIRECT".to_string(),
            "--to-port".to_string(),
            rule.to_port.to_string(),
            "-m".to_string(),
            "comment".to_string(),
            "--comment".to_string(),
            "eden-portswitch".to_string(),
        ]);

        Ok(args)
    }

    pub fn add(&self, rule: &RedirectRule) -> Result<(), RedirectError> {
        let args = self.build_rule_args(rule, "-A")?;
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.run_iptables(&args_ref)?;

        info!("Added OUTPUT rule: {} -> {} (exclude uid: {:?})", rule.from_port, rule.to_port, rule.exclude_uid);

        Ok(())
    }

    pub fn remove(&self, rule: &RedirectRule) -> Result<(), RedirectError> {
        let args = self.build_rule_args(rule, "-D")?;
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

        match self.run_iptables(&args_ref) {
            Ok(_) => {
                info!("Removed redirect: {} -> {}", rule.from_port, rule.to_port);
                Ok(())
            }
            Err(RedirectError::CommandFailed(msg)) if msg.contains("No chain/target/match") => {
                warn!("Rule not found, may have already been removed");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn list(&self) -> Result<(), RedirectError> {
        let output = self.run_iptables(&["-t", "nat", "-L", "OUTPUT", "-n", "-v", "--line-numbers"])?;

        println!("=== Eden PortSwitch Rules (OUTPUT chain) ===\n");

        let mut found = false;
        for line in output.lines() {
            if line.contains("eden-portswitch") {
                println!("{}", line);
                found = true;
            }
        }

        if !found {
            println!("No eden-portswitch rules found.");
        }

        println!("\n=== Full NAT OUTPUT chain ===\n");
        println!("{}", output);

        Ok(())
    }

    pub fn clear(&self) -> Result<(), RedirectError> {
        // Get current rules
        let output = self.run_iptables(&["-t", "nat", "-L", "OUTPUT", "-n", "--line-numbers"])?;

        // Parse line numbers for eden-portswitch rules (in reverse order to preserve numbering)
        let mut lines_to_remove: Vec<u32> = Vec::new();
        for line in output.lines() {
            if line.contains("eden-portswitch") {
                if let Some(num_str) = line.split_whitespace().next() {
                    if let Ok(num) = num_str.parse::<u32>() {
                        lines_to_remove.push(num);
                    }
                }
            }
        }

        // Remove in reverse order
        lines_to_remove.sort();
        lines_to_remove.reverse();

        for num in lines_to_remove {
            match self.run_iptables(&["-t", "nat", "-D", "OUTPUT", &num.to_string()]) {
                Ok(_) => info!("Removed rule #{}", num),
                Err(e) => warn!("Failed to remove rule #{}: {}", num, e),
            }
        }

        info!("Cleared all eden-portswitch rules");
        Ok(())
    }

    pub fn dry_run(&self, rule: &RedirectRule) -> Result<(), RedirectError> {
        let args = self.build_rule_args(rule, "-A")?;

        println!("=== Command that would be executed ===\n");
        println!("sudo iptables {}", args.join(" "));
        println!();

        if let Some(ref username) = rule.exclude_user {
            println!("# Traffic from user '{}' (uid {}) will NOT be redirected", username, rule.exclude_uid.unwrap_or(0));
            println!("# All other traffic to port {} will redirect to port {}", rule.from_port, rule.to_port);
        }

        println!("\n=== To remove this rule ===\n");
        let remove_args = self.build_rule_args(rule, "-D")?;
        println!("sudo iptables {}", remove_args.join(" "));

        Ok(())
    }

    pub fn preflight(&self, eden_port: u16, redis_port: u16, eden_user: Option<String>) -> Result<(), RedirectError> {
        println!("=== Preflight Checks ===\n");

        // Check 1: iptables available
        print!("1. iptables available... ");
        match Command::new("iptables").arg("--version").output() {
            Ok(output) if output.status.success() => {
                println!("✓");
            }
            _ => {
                println!("✗");
                return Err(RedirectError::PreflightFailed("iptables not found or not executable".to_string()));
            }
        }

        // Check 2: Can we access iptables (are we root)?
        print!("2. Root privileges... ");
        match self.run_iptables(&["-t", "nat", "-L", "OUTPUT", "-n"]) {
            Ok(_) => println!("✓"),
            Err(RedirectError::PermissionDenied) => {
                println!("✗");
                return Err(RedirectError::PreflightFailed("Not running as root. Use sudo.".to_string()));
            }
            Err(e) => {
                println!("✗");
                return Err(e);
            }
        }

        // Check 3: Eden user exists (if specified)
        if let Some(ref username) = eden_user {
            print!("3. User '{}' exists... ", username);
            match users::get_user_by_name(username) {
                Some(user) => {
                    println!("✓ (uid {})", user.uid());
                }
                None => {
                    println!("✗");
                    return Err(RedirectError::PreflightFailed(format!(
                        "User '{}' not found. Create it with: useradd -r -s /bin/false {}",
                        username, username
                    )));
                }
            }
        } else {
            println!("3. Eden user... ⚠ not specified (redirect loop possible!)");
        }

        // Check 4: Eden port is listening
        print!("4. Eden listening on port {}... ", eden_port);
        match TcpStream::connect_timeout(&format!("127.0.0.1:{}", eden_port).parse().unwrap(), Duration::from_secs(2)) {
            Ok(_) => println!("✓"),
            Err(_) => {
                println!("✗");
                return Err(RedirectError::PreflightFailed(format!(
                    "Nothing listening on port {}. Start Eden first.",
                    eden_port
                )));
            }
        }

        // Check 5: Redis port is listening
        print!("5. Redis listening on port {}... ", redis_port);
        match TcpStream::connect_timeout(&format!("127.0.0.1:{}", redis_port).parse().unwrap(), Duration::from_secs(2)) {
            Ok(_) => println!("✓"),
            Err(_) => {
                println!("✗");
                return Err(RedirectError::PreflightFailed(format!(
                    "Nothing listening on port {}. Is Redis running?",
                    redis_port
                )));
            }
        }

        println!("\n=== All checks passed ===\n");
        println!("Ready to run:");
        println!(
            "  sudo eden-portswitch add --from {} --to {}{}",
            redis_port,
            eden_port,
            eden_user.map(|u| format!(" --exclude-user {}", u)).unwrap_or_default()
        );

        Ok(())
    }
}
