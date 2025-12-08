"""
数据库回滚脚本
用于在数据修复失败时回滚到之前的状态
"""

import os
import sqlite3
import shutil
from datetime import datetime
from typing import List, Dict, Any, Optional
import json


class DatabaseRollback:
    """数据库回滚管理器"""

    def __init__(self, db_path: str = "database/accounting.db"):
        """
        初始化回滚管理器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.backup_dir = "database/backups"
        os.makedirs(self.backup_dir, exist_ok=True)

    def create_backup(self, description: str = "") -> str:
        """
        创建数据库备份

        Args:
            description: 备份描述

        Returns:
            备份文件路径
        """
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.db"

        if description:
            # 清理描述中的特殊字符
            safe_desc = "".join(c for c in description if c.isalnum() or c in (' ', '-', '_'))
            backup_filename = f"backup_{timestamp}_{safe_desc[:50]}.db"

        backup_path = os.path.join(self.backup_dir, backup_filename)

        # 复制数据库文件
        shutil.copy2(self.db_path, backup_path)

        # 创建备份元数据
        metadata = {
            'backup_timestamp': datetime.now().isoformat(),
            'original_db': self.db_path,
            'backup_path': backup_path,
            'description': description,
            'file_size': os.path.getsize(backup_path),
            'checksum': self._calculate_checksum(backup_path)
        }

        # 保存元数据
        metadata_path = backup_path + '.meta.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        print(f"[备份] 数据库已备份到: {backup_path}")
        print(f"[备份] 备份大小: {metadata['file_size']:,} 字节")

        return backup_path

    def list_backups(self) -> List[Dict[str, Any]]:
        """
        列出所有可用的备份

        Returns:
            备份列表
        """
        backups = []

        for filename in os.listdir(self.backup_dir):
            if filename.endswith('.db'):
                backup_path = os.path.join(self.backup_dir, filename)
                metadata_path = backup_path + '.meta.json'

                backup_info = {
                    'filename': filename,
                    'path': backup_path,
                    'size': os.path.getsize(backup_path),
                    'modified': datetime.fromtimestamp(os.path.getmtime(backup_path)).isoformat()
                }

                # 尝试读取元数据
                if os.path.exists(metadata_path):
                    try:
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                            backup_info.update(metadata)
                    except:
                        pass

                backups.append(backup_info)

        # 按修改时间排序（最新的在前）
        backups.sort(key=lambda x: x['modified'], reverse=True)

        return backups

    def restore_backup(self, backup_path: str, validate: bool = True) -> bool:
        """
        从备份恢复数据库

        Args:
            backup_path: 备份文件路径
            validate: 是否验证备份完整性

        Returns:
            是否恢复成功
        """
        if not os.path.exists(backup_path):
            print(f"[错误] 备份文件不存在: {backup_path}")
            return False

        # 验证备份文件
        if validate:
            if not self._validate_backup(backup_path):
                print(f"[错误] 备份文件验证失败: {backup_path}")
                return False

        # 备份当前数据库（如果存在）
        if os.path.exists(self.db_path):
            current_backup = self.create_backup("恢复前的自动备份")
            print(f"[恢复] 当前数据库已备份到: {current_backup}")

        try:
            # 恢复备份
            shutil.copy2(backup_path, self.db_path)
            print(f"[恢复] 数据库已从备份恢复: {backup_path}")

            # 验证恢复后的数据库
            if validate:
                if self._validate_database():
                    print("[验证] 数据库恢复验证通过")
                    return True
                else:
                    print("[错误] 数据库恢复验证失败")
                    # 尝试恢复自动备份
                    if 'current_backup' in locals():
                        print("[恢复] 尝试恢复自动备份...")
                        shutil.copy2(current_backup, self.db_path)
                    return False

            return True

        except Exception as e:
            print(f"[错误] 恢复失败: {e}")
            # 尝试恢复自动备份
            if 'current_backup' in locals():
                print("[恢复] 尝试恢复自动备份...")
                shutil.copy2(current_backup, self.db_path)
            return False

    def restore_latest_backup(self) -> bool:
        """
        恢复最新的备份

        Returns:
            是否恢复成功
        """
        backups = self.list_backups()

        if not backups:
            print("[错误] 没有可用的备份")
            return False

        latest_backup = backups[0]
        print(f"[恢复] 恢复最新备份: {latest_backup['filename']}")

        return self.restore_backup(latest_backup['path'])

    def restore_by_timestamp(self, timestamp: str) -> bool:
        """
        按时间戳恢复备份

        Args:
            timestamp: 时间戳（格式: YYYYMMDD_HHMMSS）

        Returns:
            是否恢复成功
        """
        # 查找匹配的备份文件
        for backup in self.list_backups():
            if timestamp in backup['filename']:
                print(f"[恢复] 找到匹配的备份: {backup['filename']}")
                return self.restore_backup(backup['path'])

        print(f"[错误] 未找到时间戳为 {timestamp} 的备份")
        return False

    def _calculate_checksum(self, filepath: str) -> str:
        """
        计算文件校验和

        Args:
            filepath: 文件路径

        Returns:
            校验和字符串
        """
        import hashlib

        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            # 逐块读取以避免内存问题
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()

    def _validate_backup(self, backup_path: str) -> bool:
        """
        验证备份文件完整性

        Args:
            backup_path: 备份文件路径

        Returns:
            是否有效
        """
        try:
            # 检查文件大小
            file_size = os.path.getsize(backup_path)
            if file_size == 0:
                print(f"[验证] 备份文件为空: {backup_path}")
                return False

            # 检查是否是有效的SQLite数据库
            conn = sqlite3.connect(backup_path)
            cursor = conn.cursor()

            # 检查是否有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            if not tables:
                print(f"[验证] 备份文件中没有表: {backup_path}")
                conn.close()
                return False

            # 检查关键表是否存在
            required_tables = ['companies', 'account_books', 'vouchers', 'voucher_details']
            table_names = [table[0] for table in tables]

            for required_table in required_tables:
                if required_table not in table_names:
                    print(f"[验证] 备份文件缺少关键表: {required_table}")
                    conn.close()
                    return False

            conn.close()
            return True

        except sqlite3.Error as e:
            print(f"[验证] 备份文件不是有效的SQLite数据库: {e}")
            return False
        except Exception as e:
            print(f"[验证] 验证备份文件时出错: {e}")
            return False

    def _validate_database(self) -> bool:
        """
        验证数据库完整性

        Returns:
            是否有效
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 检查外键约束
            cursor.execute("PRAGMA foreign_key_check")
            foreign_key_errors = cursor.fetchall()

            if foreign_key_errors:
                print(f"[验证] 发现外键约束错误: {foreign_key_errors}")
                conn.close()
                return False

            # 检查表结构完整性
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()

            if not tables:
                print("[验证] 数据库中没有表")
                conn.close()
                return False

            conn.close()
            return True

        except sqlite3.Error as e:
            print(f"[验证] 数据库验证失败: {e}")
            return False

    def cleanup_old_backups(self, keep_last_n: int = 10, keep_days: int = 30) -> Dict[str, Any]:
        """
        清理旧的备份文件

        Args:
            keep_last_n: 保留最新的N个备份
            keep_days: 保留最近N天的备份

        Returns:
            清理结果
        """
        backups = self.list_backups()
        total_backups = len(backups)

        if total_backups <= keep_last_n:
            print(f"[清理] 备份数量({total_backups})未超过保留限制({keep_last_n})，无需清理")
            return {
                'total_backups': total_backups,
                'deleted': 0,
                'kept': total_backups
            }

        deleted_count = 0
        kept_count = 0
        cutoff_date = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)

        for i, backup in enumerate(backups):
            backup_time = datetime.fromisoformat(backup['modified']).timestamp()

            # 决定是否删除
            should_delete = False

            # 规则1: 保留最新的N个
            if i >= keep_last_n:
                should_delete = True

            # 规则2: 如果超过N天，即使是最新的N个也删除
            if backup_time < cutoff_date:
                should_delete = True

            # 规则3: 总是保留最新的备份
            if i == 0:
                should_delete = False

            if should_delete:
                try:
                    # 删除数据库文件
                    os.remove(backup['path'])

                    # 删除元数据文件（如果存在）
                    metadata_path = backup['path'] + '.meta.json'
                    if os.path.exists(metadata_path):
                        os.remove(metadata_path)

                    print(f"[清理] 删除备份: {backup['filename']}")
                    deleted_count += 1

                except Exception as e:
                    print(f"[清理] 删除备份失败 {backup['filename']}: {e}")

            else:
                kept_count += 1

        print(f"[清理] 清理完成: 删除 {deleted_count} 个备份，保留 {kept_count} 个备份")

        return {
            'total_backups': total_backups,
            'deleted': deleted_count,
            'kept': kept_count
        }

    def get_backup_stats(self) -> Dict[str, Any]:
        """
        获取备份统计信息

        Returns:
            统计信息
        """
        backups = self.list_backups()

        if not backups:
            return {
                'total_backups': 0,
                'total_size': 0,
                'oldest_backup': None,
                'newest_backup': None
            }

        total_size = sum(backup['size'] for backup in backups)
        oldest_backup = min(backups, key=lambda x: x['modified'])
        newest_backup = max(backups, key=lambda x: x['modified'])

        return {
            'total_backups': len(backups),
            'total_size': total_size,
            'total_size_mb': total_size / (1024 * 1024),
            'oldest_backup': oldest_backup['modified'],
            'newest_backup': newest_backup['modified'],
            'backup_list': backups[:5]  # 只返回前5个
        }


def main():
    """主函数：管理数据库备份和恢复"""
    import argparse

    parser = argparse.ArgumentParser(description='数据库备份和恢复管理')
    subparsers = parser.add_subparsers(dest='command', help='命令')

    # 创建备份命令
    backup_parser = subparsers.add_parser('backup', help='创建备份')
    backup_parser.add_argument('--description', default='', help='备份描述')

    # 列出备份命令
    list_parser = subparsers.add_parser('list', help='列出所有备份')

    # 恢复备份命令
    restore_parser = subparsers.add_parser('restore', help='恢复备份')
    restore_group = restore_parser.add_mutually_exclusive_group(required=True)
    restore_group.add_argument('--latest', action='store_true', help='恢复最新备份')
    restore_group.add_argument('--file', help='备份文件路径')
    restore_group.add_argument('--timestamp', help='备份时间戳(YYYYMMDD_HHMMSS)')

    # 清理备份命令
    cleanup_parser = subparsers.add_parser('cleanup', help='清理旧备份')
    cleanup_parser.add_argument('--keep-last', type=int, default=10, help='保留最新的N个备份')
    cleanup_parser.add_argument('--keep-days', type=int, default=30, help='保留最近N天的备份')

    # 统计命令
    stats_parser = subparsers.add_parser('stats', help='显示备份统计')

    args = parser.parse_args()

    rollback = DatabaseRollback()

    if args.command == 'backup':
        backup_path = rollback.create_backup(args.description)
        print(f"[完成] 备份创建成功: {backup_path}")

    elif args.command == 'list':
        backups = rollback.list_backups()

        if not backups:
            print("没有可用的备份")
            return

        print(f"可用备份 ({len(backups)} 个):")
        print("-" * 80)
        for i, backup in enumerate(backups):
            size_mb = backup['size'] / (1024 * 1024)
            desc = backup.get('description', '无描述')
            print(f"{i+1:2d}. {backup['filename']}")
            print(f"     时间: {backup['modified']}")
            print(f"     大小: {size_mb:.2f} MB")
            print(f"     描述: {desc}")
            print()

    elif args.command == 'restore':
        if args.latest:
            success = rollback.restore_latest_backup()
        elif args.file:
            success = rollback.restore_backup(args.file)
        elif args.timestamp:
            success = rollback.restore_by_timestamp(args.timestamp)

        if success:
            print("[成功] 数据库恢复完成")
        else:
            print("[失败] 数据库恢复失败")

    elif args.command == 'cleanup':
        result = rollback.cleanup_old_backups(args.keep_last, args.keep_days)
        print(f"[完成] 清理完成: 删除 {result['deleted']} 个，保留 {result['kept']} 个")

    elif args.command == 'stats':
        stats = rollback.get_backup_stats()

        print("备份统计信息:")
        print(f"  备份总数: {stats['total_backups']}")
        print(f"  总大小: {stats['total_size_mb']:.2f} MB")
        print(f"  最新备份: {stats['newest_backup']}")
        print(f"  最旧备份: {stats['oldest_backup']}")

        if stats['backup_list']:
            print("\n最近的备份:")
            for backup in stats['backup_list']:
                size_mb = backup['size'] / (1024 * 1024)
                print(f"  - {backup['filename']} ({size_mb:.2f} MB)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()