-- SQLite数据库表结构创建脚本
-- 适配SQLite的数据类型和约束

-- 1. 核心交易事实表
CREATE TABLE IF NOT EXISTS f_accounting_transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    voucher_id TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    date TEXT NOT NULL,  -- SQLite使用TEXT存储日期，格式YYYY-MM-DD
    amount REAL NOT NULL,  -- SQLite使用REAL存储浮点数
    debit_credit_flag TEXT NOT NULL CHECK (debit_credit_flag IN ('D', 'C')),
    description TEXT,
    currency TEXT DEFAULT '人民币',
    exchange_rate REAL DEFAULT 1.0,
    original_amount REAL,
    created_time TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_time TEXT DEFAULT CURRENT_TIMESTAMP,

    -- 复合索引
    UNIQUE(voucher_id, line_number)
);

-- 2. 凭证维度表
CREATE TABLE IF NOT EXISTS d_vouchers (
    voucher_id TEXT PRIMARY KEY,
    voucher_date TEXT NOT NULL,
    voucher_type TEXT NOT NULL CHECK (voucher_type IN ('银付', '银收', '现付', '现收', '转')),
    voucher_number INTEGER NOT NULL,
    company_id INTEGER,
    ledger_id INTEGER,
    description TEXT,
    total_amount REAL,
    status TEXT DEFAULT 'posted' CHECK (status IN ('draft', 'posted', 'cancelled')),
    created_by TEXT,
    approved_by TEXT,
    created_time TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 3. 科目维度表
CREATE TABLE IF NOT EXISTS d_accounts (
    account_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_code TEXT NOT NULL UNIQUE,
    account_name TEXT NOT NULL,
    parent_id INTEGER,
    level INTEGER NOT NULL,
    is_leaf BOOLEAN DEFAULT 1,  -- SQLite中布尔值用0/1表示
    account_type TEXT,
    account_direction TEXT CHECK (account_direction IN ('D', 'C')),
    is_active BOOLEAN DEFAULT 1,

    FOREIGN KEY (parent_id) REFERENCES d_accounts(account_id)
);

-- 4. 公司维度表
CREATE TABLE IF NOT EXISTS d_companies (
    company_id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_code TEXT NOT NULL UNIQUE,
    company_name TEXT NOT NULL,
    company_type TEXT,
    is_active BOOLEAN DEFAULT 1
);

-- 5. 账簿维度表
CREATE TABLE IF NOT EXISTS d_ledgers (
    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ledger_code TEXT NOT NULL UNIQUE,
    ledger_name TEXT NOT NULL,
    company_id INTEGER NOT NULL,
    ledger_type TEXT,
    currency TEXT DEFAULT '人民币',

    FOREIGN KEY (company_id) REFERENCES d_companies(company_id)
);

-- 6. 银行账户维度表
CREATE TABLE IF NOT EXISTS d_bank_accounts (
    bank_account_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT NOT NULL,
    bank_name TEXT NOT NULL,
    branch_name TEXT,
    company_id INTEGER,
    currency TEXT DEFAULT '人民币',
    is_active BOOLEAN DEFAULT 1,

    FOREIGN KEY (company_id) REFERENCES d_companies(company_id)
);

-- 7. 交易辅助维度关联表
CREATE TABLE IF NOT EXISTS r_transaction_dimensions (
    transaction_id INTEGER NOT NULL,
    dimension_type TEXT NOT NULL CHECK (dimension_type IN ('department', 'employee', 'vendor', 'customer', 'project', 'bank_account')),
    dimension_id INTEGER NOT NULL,
    dimension_value TEXT,

    PRIMARY KEY (transaction_id, dimension_type, dimension_id),
    FOREIGN KEY (transaction_id) REFERENCES f_accounting_transactions(transaction_id)
);

-- 8. 部门维度表
CREATE TABLE IF NOT EXISTS d_departments (
    dept_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dept_code TEXT NOT NULL UNIQUE,
    dept_name TEXT NOT NULL,
    parent_id INTEGER,
    level INTEGER NOT NULL,
    company_id INTEGER,

    FOREIGN KEY (parent_id) REFERENCES d_departments(dept_id),
    FOREIGN KEY (company_id) REFERENCES d_companies(company_id)
);

-- 9. 人员维度表
CREATE TABLE IF NOT EXISTS d_employees (
    employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_code TEXT NOT NULL UNIQUE,
    employee_name TEXT NOT NULL,
    dept_id INTEGER,
    position TEXT,

    FOREIGN KEY (dept_id) REFERENCES d_departments(dept_id)
);

-- 10. 客商维度表
CREATE TABLE IF NOT EXISTS d_vendors_customers (
    party_id INTEGER PRIMARY KEY AUTOINCREMENT,
    party_code TEXT NOT NULL UNIQUE,
    party_name TEXT NOT NULL,
    party_type TEXT NOT NULL CHECK (party_type IN ('vendor', 'customer', 'both')),
    tax_number TEXT,
    contact_info TEXT
);

-- 11. 项目维度表
CREATE TABLE IF NOT EXISTS d_projects (
    project_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL UNIQUE,
    project_name TEXT NOT NULL,
    project_type TEXT,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'completed', 'suspended', 'cancelled')),
    start_date TEXT,
    end_date TEXT
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_fact_voucher_line ON f_accounting_transactions(voucher_id, line_number);
CREATE INDEX IF NOT EXISTS idx_fact_account_date ON f_accounting_transactions(account_id, date);
CREATE INDEX IF NOT EXISTS idx_fact_date_account ON f_accounting_transactions(date, account_id);
CREATE INDEX IF NOT EXISTS idx_fact_amount ON f_accounting_transactions(amount);
CREATE INDEX IF NOT EXISTS idx_fact_debit_credit ON f_accounting_transactions(debit_credit_flag);
CREATE INDEX IF NOT EXISTS idx_fact_date ON f_accounting_transactions(date);

CREATE INDEX IF NOT EXISTS idx_voucher_date_type ON d_vouchers(voucher_date, voucher_type);
CREATE INDEX IF NOT EXISTS idx_voucher_company_date ON d_vouchers(company_id, voucher_date);
CREATE INDEX IF NOT EXISTS idx_voucher_number ON d_vouchers(voucher_number);

CREATE INDEX IF NOT EXISTS idx_account_code ON d_accounts(account_code);
CREATE INDEX IF NOT EXISTS idx_account_parent ON d_accounts(parent_id);
CREATE INDEX IF NOT EXISTS idx_account_level_type ON d_accounts(level, account_type);

CREATE INDEX IF NOT EXISTS idx_dimension_type_id ON r_transaction_dimensions(dimension_type, dimension_id);
CREATE INDEX IF NOT EXISTS idx_dimension_transaction ON r_transaction_dimensions(transaction_id);

CREATE INDEX IF NOT EXISTS idx_dept_company ON d_departments(company_id);
CREATE INDEX IF NOT EXISTS idx_dept_parent ON d_departments(parent_id);

CREATE INDEX IF NOT EXISTS idx_employee_dept ON d_employees(dept_id);
CREATE INDEX IF NOT EXISTS idx_employee_name ON d_employees(employee_name);

CREATE INDEX IF NOT EXISTS idx_vendor_customer_name ON d_vendors_customers(party_name);
CREATE INDEX IF NOT EXISTS idx_vendor_customer_type ON d_vendors_customers(party_type);

CREATE INDEX IF NOT EXISTS idx_project_name ON d_projects(project_name);
CREATE INDEX IF NOT EXISTS idx_project_type ON d_projects(project_type);
CREATE INDEX IF NOT EXISTS idx_project_status ON d_projects(status);