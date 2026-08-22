# proxies_config.py

# 定义代理设置
# 如果你的 SSH SOCKS 代理运行在本地的 127.0.0.1:1080
# 否则设置为 None

# 示例：代理已配置
PROXY_SETTINGS = {
    'http': 'socks5://127.0.0.1:1080',
    'https': 'socks5://127.0.0.1:1080'
}

# 示例：没有代理配置（或代理不活跃时）
# PROXY_SETTINGS = None

# 如果你有多个代理地址，也可以这样做：
# PROXY_SETTINGS_A = {
#     'http': 'socks5://127.0.0.1:1080',
#     'https': 'socks5://127.0.0.1:1080'
# }
# PROXY_SETTINGS_B = {
#     'http': 'socks5://127.0.0.1:1081',
#     'https': 'socks5://127.0.0.1:1081'
# }