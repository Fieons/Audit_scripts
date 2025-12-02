-- CSV序时账转数据库 - 建表脚本
-- 版本：1.0
-- 创建日期：2025-12-02

-- 公司表
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(50) UNIQUE
);

-- 账簿表
CREATE TABLE account_books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    name VARCHAR(200) NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- 会计科目表
CREATE TABLE account_subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    full_name VARCHAR(500),
    level INTEGER,
    subject_type VARCHAR(20),
    normal_balance VARCHAR(10)
);

-- 凭证主表
CREATE TABLE vouchers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id INTEGER NOT NULL,
    voucher_number VARCHAR(50) NOT NULL,
    voucher_type VARCHAR(10),
    voucher_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    total_debit DECIMAL(15,2) DEFAULT 0,
    total_credit DECIMAL(15,2) DEFAULT 0,
    FOREIGN KEY (book_id) REFERENCES account_books(id) ON DELETE CASCADE
);

-- 凭证明细表
CREATE TABLE voucher_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    voucher_id INTEGER NOT NULL,
    entry_number INTEGER NOT NULL,
    summary TEXT,
    subject_id INTEGER NOT NULL,
    currency VARCHAR(20),
    debit_amount DECIMAL(15,2) DEFAULT 0,
    credit_amount DECIMAL(15,2) DEFAULT 0,
    auxiliary_info TEXT,
    write_off_info TEXT,
    settlement_info TEXT,
    FOREIGN KEY (voucher_id) REFERENCES vouchers(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_id) REFERENCES account_subjects(id)
);

-- 辅助项解析表
CREATE TABLE auxiliary_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    detail_id INTEGER NOT NULL,
    item_type VARCHAR(50) NOT NULL,
    item_name VARCHAR(100),
    item_value VARCHAR(500) NOT NULL,
    FOREIGN KEY (detail_id) REFERENCES voucher_details(id) ON DELETE CASCADE
);

-- 项目表（可选）
CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code VARCHAR(50) UNIQUE,
    project_name VARCHAR(200) NOT NULL,
    company_id INTEGER,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);

-- 客商表（可选）
CREATE TABLE suppliers_customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(200) NOT NULL,
    type VARCHAR(20)
);

-- 创建索引以提高查询性能
CREATE INDEX idx_account_books_company ON account_books(company_id);

CREATE INDEX idx_vouchers_book_date ON vouchers(book_id, voucher_date);
CREATE INDEX idx_vouchers_number ON vouchers(voucher_number);

CREATE INDEX idx_voucher_details_voucher ON voucher_details(voucher_id);
CREATE INDEX idx_voucher_details_subject ON voucher_details(subject_id);

CREATE INDEX idx_auxiliary_items_detail ON auxiliary_items(detail_id);
CREATE INDEX idx_auxiliary_items_type_value ON auxiliary_items(item_type, item_value);

-- 注释说明
-- 1. 所有金额字段使用DECIMAL(15,2)类型，支持最大999,999,999,999.99
-- 2. 外键约束使用ON DELETE CASCADE确保数据完整性
-- 3. 索引设计优化了常用查询场景
-- 4. 可选表（projects, suppliers_customers）可根据实际需求决定是否创建

-- 使用说明：
-- 1. 执行此脚本创建数据库表结构
-- 2. 建议先创建companies和account_subjects表并插入基础数据
-- 3. 然后按照业务顺序插入数据：companies → account_books → vouchers → voucher_details → auxiliary_items
-- 4. 可选表可在需要时创建和填充