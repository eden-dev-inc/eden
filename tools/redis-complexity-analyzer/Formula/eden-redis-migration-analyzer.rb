class EdenRedisMigrationAnalyzer < Formula
  desc "Analyze Redis database complexity, estimate Azure migration pricing and timeline"
  homepage "https://www.eden.dev/migrate/redis"
  # Update this URL and sha256 when publishing a release
  url "https://github.com/eden-platform/eden/archive/refs/tags/analyzer-v0.1.0.tar.gz"
  sha256 "UPDATE_WITH_ACTUAL_SHA256"
  license "MIT"

  depends_on "rust" => :build

  def install
    cd "tools/redis-complexity-analyzer" do
      system "cargo", "install", *std_cargo_args
    end
  end

  test do
    assert_match "Eden", shell_output("#{bin}/eden-redis-migration-analyzer --help")
  end
end
