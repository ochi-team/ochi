pub const TenantID = []const u8;

pub const maxTenantIDLen = 16;

pub fn checkID(value: TenantID) bool {
    return value.len <= maxTenantIDLen;
}
