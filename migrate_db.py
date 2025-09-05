"""
Скрипт для обновления схемы базы данных
Добавляет отсутствующие колонки в существующую базу данных
"""
import sqlite3
import os

def migrate_database():
    # Путь к базе данных
    db_path = os.path.join(os.path.dirname(__file__), 'instance', 'proxmox_clusters.db')
    
    if not os.path.exists(db_path):
        print("База данных не найдена, будет создана автоматически при запуске приложения")
        return
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Проверяем, есть ли колонка is_first_login в таблице admin_user
        cursor.execute("PRAGMA table_info(admin_user)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'is_first_login' not in columns:
            print("Добавляем колонку is_first_login в таблицу admin_user...")
            cursor.execute("ALTER TABLE admin_user ADD COLUMN is_first_login BOOLEAN DEFAULT 1")
            print("Колонка добавлена успешно!")
        else:
            print("Колонка is_first_login уже существует")
        
        conn.commit()
        print("Миграция завершена успешно!")
        
    except Exception as e:
        print(f"Ошибка при миграции: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database()
