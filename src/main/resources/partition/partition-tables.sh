#!/bin/bash
_HOME=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)

# Load environment variables from .env file
ENV_FILE="$_HOME/.env"
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "Environment file not found: $ENV_FILE"
    exit 1
fi

###############################
#        CONFIGURATION        #
###############################

# Database
dbhost="$DB_HOST"
dbuser="$DB_USER"
dbpass="$DB_PASS"
dbname="$DB_NAME"
timezone="$TIMEZONE"

# Dry run flag (default: false)
DRY_RUN=false

# "table_name:partition_type"
# partition_type h = hourly, d = daily
tables=("cdr:d")

# Days ahead for creation tables
days_ahead=1
# Days ago for drop tables
days_ago=7

# Logs
logs_path=$_HOME/logs
log_file=$logs_path/db_$(date "+%Y%m%d").log

###############################
#          FUNCTIONS          #
###############################

log() {
    message=$1
    dt=$(date "+%Y-%m-%d %H:%M:%S,%3N")
    echo "$dt -> $message" >> $log_file
}

execute_ddl() {
    query=$1
    echo "$query"
    if [ -z "$query" ]; then
        echo "Nothing to execute, query not specified"
        return
    fi
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] Query not executed."
        return
    fi
    PGPASSWORD="$dbpass" psql -h "$dbhost" -U "$dbuser" -d "$dbname" -tAc "$query"
}


daily_table() {
    table=$1
    t_date=$2
    action=$3
    query=""
    if [ $t_date -eq 0 ]; then return; fi

    if [ "$action" == "create" ]; then
        from=$(TZ=$timezone date -d "$t_date" '+%Y-%m-%d 00:00:00')
        to=$(TZ=$timezone date -d "$t_date +1 day" '+%Y-%m-%d 00:00:00')
        part_name="${table}_${t_date}"
        query="CREATE TABLE IF NOT EXISTS $part_name PARTITION OF $table FOR VALUES FROM (TIMESTAMP '$from') TO (TIMESTAMP '$to');"
        result=$(execute_ddl "$query")
        log "$result"

        # Create index
        index_list=(
          "message_type"
          "status"
          "origination_type"
          "origination_protocol"
          "destination_protocol"
          "destination_type"
          "addr_src_digits"
          "addr_dst_digits"
          "origination_network_id"
          "destination_network_id"
          "registered_delivery"
          "broadcast_id"
        )

        for col in "${index_list[@]}"; do
            index_name="idx_${part_name}_${col}"
            idx_query="CREATE INDEX IF NOT EXISTS $index_name ON $part_name ($col);"
            idx_result=$(execute_ddl "$idx_query")
            log "Index $index_name: $idx_result"
        done

    elif [ "$action" == "drop" ]; then
        part_name="${table}_${t_date}"
        query="DROP TABLE IF EXISTS $part_name;"
        result=$(execute_ddl "$query")
        log "$result"
    fi
}


hourly_tables() {
    table=$1
    t_date=$2
    action=$3
    if [ $t_date -eq 0 ]; then return; fi

    for i in {0..23}; do
        query=""
        if [ $i -lt 10 ]; then
            hour="0$i"
        else
            hour="$i"
        fi

        from=$(TZ=$timezone date -d "$t_date $hour:00:00" '+%Y-%m-%d %H:%M:%S')
        to=$(TZ=$timezone date -d "$t_date $hour:00:00 +1 hour" '+%Y-%m-%d %H:%M:%S')
        part_name="${table}_${t_date}_${hour}"

        if [ "$action" == "create" ]; then
            query="CREATE TABLE IF NOT EXISTS $part_name PARTITION OF $table FOR VALUES FROM (TIMESTAMP '$from') TO (TIMESTAMP '$to');"

            result=$(execute_ddl "$query")
            log "$result"

            # Create indexes for partition
            index_list=(
              "message_type"
              "status"
              "origination_type"
              "origination_protocol"
              "destination_protocol"
              "destination_type"
              "addr_src_digits"
              "addr_dst_digits"
              "origination_network_id"
              "destination_network_id"
              "registered_delivery"
              "broadcast_id"
            )

            for col in "${index_list[@]}"; do
                index_name="idx_${part_name}_${col}"
                idx_query="CREATE INDEX IF NOT EXISTS $index_name ON $part_name ($col);"
                idx_result=$(execute_ddl "$idx_query")
                log "INDEX $index_name: $idx_result"
            done

        elif [ "$action" == "drop" ]; then
            query="DROP TABLE IF EXISTS $part_name;"
            result=$(execute_ddl "$query")
            log "$result"
        fi
    done
}



###############################
#            MAIN             #
###############################

if [ ! -e "$logs_path" ]; then
    mkdir -p $logs_path
fi

t_date_ahead=0
t_date_ago=0
action=""

if [ $# -eq 0 ]; then
    t_date_ahead=$(TZ=$timezone date -d "+ $days_ahead day" '+%Y%m%d')
    t_date_ago=$(TZ=$timezone date -d "$days_ago day ago" '+%Y%m%d')
elif [ $# -eq 1 ]; then
    hours=$1
    sx_temp=$(date -d "$hours hour" "+%Y%m%d_%H")
    date_temp=$(date -d "$hours hour" "+%Y%m%d %H")
    epoch_temp=$(date -d "$date_temp" "+%s")
    from=$epoch_temp
    to=$((epoch_temp+3600))
    for x in ${tables[@]}; do
        table=$(echo $x | awk -F":" '{print $1}')
        if [ $hours -lt 0 ]; then
            query="drop table if exists $table"_"$sx_temp"
            result=$(execute_ddl "$query")
            log "$result"
        elif [ $hours -gt 0 ]; then
            query="create table if not exists $table"_"$sx_temp partition of $table for values from ($from) to ($to)"
            result=$(execute_ddl "$query")
            log "$result"
        fi
    done
    # date_temp=$(date -d "$hours hour" "+%Y%m%d_%H")
    # epoch_temp=$(date -d "$hours hour" "+%s")
elif [ $# -eq 2 ]; then
    if [[ $1 =~ ^[0-9]{4}[0-1][0-9][0-3][0-9]$ ]]; then
        date -d "$date_param" >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            if [ "$2" == "create" ]; then
                t_date_ahead=$1
                t_date_ago=0
            elif [ "$2" == "drop" ]; then
                t_date_ahead=0
                t_date_ago=$1
            else
                echo "Invalid action: $2"
                exit 1
            fi
        else
            echo "Invalid date: $1"
            exit 1
        fi
    else
        echo "Invalid format: $1"
        exit 1
    fi
else
    echo "Invalid number of parameters: $#"
    exit 1
fi

# dropping historical tables from hours/days ago
# and creating partitioned tables for hours/days ahead

for x in ${tables[@]}; do
    table=$(echo $x | awk -F":" '{print $1}')
    ptype=$(echo $x | awk -F":" '{print $2}')
    echo $table
    if [ "$ptype" == "h" ]; then
        hourly_tables $table $t_date_ahead "create"
        hourly_tables $table $t_date_ago "drop"
    elif [ "$ptype" == "d" ]; then
        daily_table $table $t_date_ahead "create"
        daily_table $table $t_date_ago "drop"
    fi
done