#!/bin/bash

# Настройки
BACKUP_DIR="./backups"
CONTAINER_NAME="metabase"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Создаем директорию для бэкапов
mkdir -p "$BACKUP_DIR"

echo "🔍 Проверяем контейнер Metabase..."
if ! docker ps -a | grep -q "$CONTAINER_NAME"; then
    echo "❌ Контейнер $CONTAINER_NAME не найден!"
    exit 1
fi

echo "🔍 Ищем файл базы данных..."
DB_FILE=$(docker exec "$CONTAINER_NAME" find / -name "metabase.db.mv.db" 2>/dev/null | head -1)

if [ -z "$DB_FILE" ]; then
    echo "❌ Файл базы данных не найден в контейнере!"
    echo "📋 Ищем все файлы .mv.db:"
    docker exec "$CONTAINER_NAME" find / -name "metabase.db.mv.db" 2>/dev/null
    exit 1
fi

echo "📦 Найден файл базы: $DB_FILE"

# Создаем имя для бэкапа
BACKUP_FILE="$BACKUP_DIR/metabase_backup_$TIMESTAMP.db.mv.db"

echo "⏹️ Останавливаем Metabase..."
docker stop "$CONTAINER_NAME"

echo "📦 Создаем бэкап..."
docker cp "$CONTAINER_NAME:$DB_FILE" "$BACKUP_FILE"

echo "▶️ Запускаем Metabase..."
docker start "$CONTAINER_NAME"

# Проверяем что бэкап создан
if [ -f "$BACKUP_FILE" ]; then
    echo "✅ Бэкап успешно создан: $BACKUP_FILE"
    echo "📊 Размер: $(du -h "$BACKUP_FILE" | cut -f1)"

    # Показываем список всех бэкапов
    echo ""
    echo "📋 Список всех бэкапов:"
    ls -la "$BACKUP_DIR"/*db.mv.db 2>/dev/null | awk '{print $9 " (" $5 " bytes)"}'
else
    echo "❌ Ошибка при создании бэкапа!"
    exit 1
fi