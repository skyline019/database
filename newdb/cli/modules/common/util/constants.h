#pragma once

#include <string>
#include <cstdint>

/**
 * @brief 数据库常量定义类
 * 
 * 集中管理所有 magic numbers 和配置常量，便于维护和修改
 */
class Constants {
public:
    // ==================== 文件相关常量 ====================
    
    /// 默认数据文件扩展名
    static constexpr const char* DATA_FILE_EXT = ".bin";
    
    /// 属性文件扩展名
    static constexpr const char* ATTR_FILE_EXT = ".attr";
    
    /// 日志文件扩展名
    static constexpr const char* LOG_FILE_EXT = ".log";
    
    /// WAL 文件扩展名
    static constexpr const char* WAL_FILE_EXT = ".wal";
    
    /// 临时文件扩展名
    static constexpr const char* TEMP_FILE_EXT = ".tmp";
    
    /// 默认表文件名
    static constexpr const char* DEFAULT_TABLE_NAME = "users";
    
    /// 日志文件名
    static constexpr const char* DEFAULT_LOG_FILE = "demo_log.bin";
    
    // ==================== 加密相关常量 ====================
    
    /// 属性文件 XOR 密钥
    static constexpr std::uint8_t ATTR_XOR_KEY = 0x5A;
    
    /// 日志文件 XOR 密钥
    static constexpr std::uint8_t LOG_XOR_KEY = 0x5A;
    
    // ==================== 分页相关常量 ====================
    
    /// 默认页大小
    static constexpr std::size_t DEFAULT_PAGE_SIZE = 4096;
    
    /// 默认页码
    static constexpr std::size_t DEFAULT_PAGE_NUMBER = 1;
    
    /// 默认每页记录数
    static constexpr std::size_t DEFAULT_PAGE_SIZE_RECORDS = 10;
    
    /// 最大列宽
    static constexpr std::size_t MAX_COLUMN_WIDTH = 32;
    
    // ==================== 缓冲区大小 ====================
    
    /// 日志缓冲区大小
    static constexpr std::size_t LOG_BUFFER_SIZE = 1024;
    
    /// 命令缓冲区大小
    static constexpr std::size_t COMMAND_BUFFER_SIZE = 512;
    
    /// 临时缓冲区大小
    static constexpr std::size_t TEMP_BUFFER_SIZE = 1024;
    
    // ==================== UI 相关常量 ====================
    
    /// 面板最小宽度
    static constexpr int MIN_PANEL_WIDTH = 200;
    
    /// 面板最大宽度
    static constexpr int MAX_PANEL_WIDTH = 280;
    
    /// 状态栏日期时间格式
    static constexpr const char* DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    
    /// 表格默认行高
    static constexpr int DEFAULT_ROW_HEIGHT = 28;
    
    /// 文本编辑默认字体大小
    static constexpr int DEFAULT_FONT_SIZE = 10;
    
    /// 等宽字体名称
    static constexpr const char* MONOSPACE_FONT = "Consolas";
    
    /// MDB 编辑器字体大小
    static constexpr int MDB_EDITOR_FONT_SIZE = 11;
    
    // ==================== 线程相关常量 ====================
    
    /// VACUUM 线程休眠间隔（秒）
    static constexpr int VACUUM_THREAD_INTERVAL_SEC = 60;
    
    /// 锁等待超时（毫秒）
    static constexpr int LOCK_WAIT_TIMEOUT_MS = 5000;
    
    // ==================== 验证相关常量 ====================
    
    /// 日期字符串长度
    static constexpr std::size_t DATE_STRING_LENGTH = 10;
    
    /// 日期时间字符串长度
    static constexpr std::size_t DATETIME_STRING_LENGTH = 19;
    
    /// 最大属性文件长度
    static constexpr std::uint32_t MAX_ATTR_FILE_LENGTH = 65536;
    
    // ==================== 颜色主题（CSS） ====================
    
    // 主色调
    static constexpr const char* COLOR_PRIMARY = "#3498db";
    static constexpr const char* COLOR_PRIMARY_HOVER = "#2980b9";
    static constexpr const char* COLOR_SUCCESS = "#2ecc71";
    static constexpr const char* COLOR_SUCCESS_HOVER = "#229954";
    static constexpr const char* COLOR_WARNING = "#f39c12";
    static constexpr const char* COLOR_DANGER = "#e74c3c";
    static constexpr const char* COLOR_DANGER_HOVER = "#c0392b";
    static constexpr const char* COLOR_INFO = "#9b59b6";
    static constexpr const char* COLOR_INFO_HOVER = "#8e44ad";
    
    // 背景色
    static constexpr const char* COLOR_BG_LIGHT = "#ecf0f1";
    static constexpr const char* COLOR_BG_WHITE = "#ffffff";
    static constexpr const char* COLOR_BG_DARK = "#34495e";
    static constexpr const char* COLOR_BG_GRAY = "#d5dbdb";
    
    // 边框色
    static constexpr const char* COLOR_BORDER = "#bdc3c7";
    static constexpr const char* COLOR_BORDER_LIGHT = "#ecf0f1";
    
    // 文字色
    static constexpr const char* COLOR_TEXT_PRIMARY = "#2c3e50";
    static constexpr const char* COLOR_TEXT_SECONDARY = "#7f8c8d";
    static constexpr const char* COLOR_TEXT_LIGHT = "#d4d4d4";
    
    // ==================== 布局间距 ====================
    
    /// 默认边距
    static constexpr int DEFAULT_MARGIN = 8;
    
    /// 大边距
    static constexpr int LARGE_MARGIN = 12;
    
    /// 小边距
    static constexpr int SMALL_MARGIN = 4;
    
    /// 默认间距
    static constexpr int DEFAULT_SPACING = 8;
    
    /// 小间距
    static constexpr int SMALL_SPACING = 4;
    
    /// 大间距
    static constexpr int LARGE_SPACING = 12;
    
    // ==================== 圆角 ====================
    
    /// 小圆角
    static constexpr int BORDER_RADIUS_SMALL = 4;
    
    /// 中圆角
    static constexpr int BORDER_RADIUS_MEDIUM = 6;
    
    /// 大圆角
    static constexpr int BORDER_RADIUS_LARGE = 8;
    
    // ==================== 通用方法 ====================
    
    /// 获取完整的数据文件名
    static std::string getDataFileName(const std::string& tableName) {
        return tableName + DATA_FILE_EXT;
    }
    
    /// 获取完整的属性文件名
    static std::string getAttrFileName(const std::string& tableName) {
        return tableName + ATTR_FILE_EXT;
    }
    
    /// 获取完整的日志文件名
    static std::string getLogFileName(const std::string& baseName) {
        return baseName + LOG_FILE_EXT;
    }
    
    /// 获取完整的 WAL 文件名
    static std::string getWalFileName(const std::string& tableName) {
        return tableName + WAL_FILE_EXT;
    }
    
    /// 获取临时文件名
    static std::string getTempFileName(const std::string& originalPath) {
        return originalPath + TEMP_FILE_EXT;
    }
};

namespace dbconst {
    // 主色调
    constexpr const char* COLOR_PRIMARY = Constants::COLOR_PRIMARY;
    constexpr const char* COLOR_PRIMARY_HOVER = Constants::COLOR_PRIMARY_HOVER;
    // ... 其他常量
}
