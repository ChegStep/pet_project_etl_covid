#!/bin/bash

# Настройки
BACKUP_DIR="./backups"
CONTAINER_NAME="metabase"

# Проверяем что директория с бэкапами существует
if [ ! -d "$BACKUP_DIR" ]; then
    echo "❌ Директория с бэкапами не найдена: $BACKUP_DIR"
    exit 1
fi

# Получаем список бэкапов
BACKUPS=($(ls "$BACKUP_DIR"/*db.mv.db 2>/dev/null))

if [ ${#BACKUPS[@]} -eq 0 ]; then
    echo "❌ Нет доступных бэкапов в директории $BACKUP_DIR"
    exit 1
fi

echo "📋 Доступные бэкапы:"
echo ""

for i in "${!BACKUPS[@]}"; do
    size=$(du -h "${BACKUPS[$i]}" | cut -f1)
    echo "$((i+1)). ${BACKUPS[$i]} ($size)"
done

echo ""
read -p "🔢 Выберите номер бэкапа для восстановления (1-${#BACKUPS[@]}): " choice

# Проверяем выбор
if ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$choice" -lt 1 ] || [ "$choice" -gt "${#BACKUPS[@]}" ]; then
    echo "❌ Неверный выбор!"
    exit 1
fi

SELECTED_BACKUP="${BACKUPS[$((choice-1))]}"
DB_FILE=$(docker exec "$CONTAINER_NAME" find / -name "metabase.db.mv.db" 2>/dev/null | head -1)

if [ -z "$DB_FILE" ]; then
    DB_FILE="metabase.db.mv.db"
    echo "⚠️  Файл базы не найден, используем путь по умолчанию: $DB_FILE"
fi

echo ""
echo "⚠️  ВНИМАНИЕ: Это перезапишет текущую базу данных!"
read -p "❓ Вы уверены что хотите восстановить из $SELECTED_BACKUP? (y/N): " confirm

if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "❌ Восстановление отменено"
    exit 0
fi

echo "⏹️ Останавливаем Metabase..."
docker stop "$CONTAINER_NAME"

echo "📦 Восстанавливаем базу данных..."
docker cp "$SELECTED_BACKUP" "$CONTAINER_NAME:$DB_FILE"

echo "▶️ Запускаем Metabase..."
docker start "$CONTAINER_NAME"

echo "✅ Восстановление завершено!"
echo "📊 Восстановлено из: $SELECTED_BACKUP"
echo "📋 Логи: docker logs $CONTAINER_NAME"