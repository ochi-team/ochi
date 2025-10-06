pub const AppConfig = struct {
    maxRequestSize: u32,
};

pub const Conf = struct {
    port: u16,

    app: AppConfig,

    // TODO: implement yaml/toml/etc. based config
    pub fn default() Conf {
        return Conf{
            .port = 9012,

            .app = .{
                .maxRequestSize = 1024 * 1024 * 4,
            },
        };
    }
};
