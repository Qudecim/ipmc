package ipmc

type Config struct {
	Binlog_directory   string
	Binlog_max_writes  int
	Snapshot_directory string
}

func NewConfig(binlog_directory string, binlog_max_writes int, snapshot_directory string) *Config {
	return &Config{
		Binlog_directory:   binlog_directory,
		Binlog_max_writes:  binlog_max_writes,
		Snapshot_directory: snapshot_directory,
	}
}
