from model.db import get_connection  # Reusing your DB connection function


def create_application_schema():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # ---------------- plants ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS plants (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT,
            capacity_kw DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)


        # ================= USER DASHBOARDS =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_dashboards (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            dashboard_id TEXT,
            dashboard_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

            # ================= USER WIDGETS NEW =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_widgets_new (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            widget_name TEXT,
            dashboard_id TEXT,
            config JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # ---------------- users ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone CHARACTER VARYING UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,

            is_active BOOLEAN DEFAULT TRUE,
            is_blocked BOOLEAN DEFAULT FALSE,

            failed_attempts INTEGER DEFAULT 0,
            last_failed_login TIMESTAMP,
            lock_until TIMESTAMP,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_updated_at TIMESTAMP;")

        # ---------------- roles ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL CHECK (name <> '')
        );
        """)

        #  SEED ROLES FIRST (VERY IMPORTANT)
        cursor.execute("""
            INSERT INTO roles(name)
            VALUES
              ('admin'),
              ('manager'),
              ('supervisor'),
              ('engineer'),
              ('operator'),
              ('guest'),
              ('cogneta')
            ON CONFLICT (name) DO NOTHING;
        """)

        # ---------------- user_roles ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            role_id INTEGER REFERENCES roles(id) ON DELETE CASCADE,
            PRIMARY KEY (user_id, role_id)
        );
        """)

        # ---------------- devices ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id SERIAL PRIMARY KEY,
            device_id TEXT UNIQUE NOT NULL,
            device_type TEXT NOT NULL,
            plant_id INTEGER REFERENCES plants(id) ON DELETE SET NULL
        );
        """)

        # ---------------- energy_strategy ----------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS energy_strategy (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                start_ts TIMESTAMP,
                end_ts TIMESTAMP,
                type VARCHAR,
                tariff FLOAT,
                u_tariff VARCHAR,
                power FLOAT,
                duration VARCHAR,
                min_soc INT,
                max_soc INT,
                priority VARCHAR CHECK (priority IN ('low','medium','high')),
                schedule_from VARCHAR,
                remark VARCHAR,
                status VARCHAR
            );
        """)

        cursor.execute("ALTER TABLE energy_strategy ADD COLUMN IF NOT EXISTS state TEXT;")

        # -------------------- notifications for EMS ------------------
        cursor.execute("""
            CREATE TABLE notification_event (

                id BIGSERIAL PRIMARY KEY,

                event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                source TEXT NOT NULL,

                event_type TEXT NOT NULL,

                severity TEXT NOT NULL,

                title TEXT NOT NULL,

                message TEXT,

                entity_type TEXT,

                entity_id TEXT,

                status TEXT DEFAULT 'active',

                acknowledged BOOLEAN DEFAULT FALSE,

                acknowledged_by TEXT,

                acknowledged_time TIMESTAMPTZ,

                cleared_time TIMESTAMPTZ,

                metadata JSONB,

                created_at TIMESTAMPTZ DEFAULT NOW()

            );
            CREATE INDEX idx_notification_time
            ON notification_event (event_time);

            CREATE INDEX idx_notification_status
            ON notification_event (status);

            CREATE INDEX idx_notification_severity
            ON notification_event (severity);

            CREATE INDEX idx_notification_source
            ON notification_event (source);

            CREATE INDEX idx_notification_entity
            ON notification_event (entity_type, entity_id);

            CREATE UNIQUE INDEX uniq_active_notification
            ON notification_event (

                source,
                event_type,
                entity_id,
                message

            )
            WHERE status = 'active';
        """)

        # ---------------- state_tariff_snapshot ----------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS state_tariff_snapshot (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP NOT NULL,
                state_code TEXT NOT NULL,
                state_name TEXT,
                tariff DOUBLE PRECISION,
                unit TEXT,
                source TEXT DEFAULT 'vidyutpravah.in'
            );
        """)

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS state_tariff_snapshot_ts_code_uniq
            ON state_tariff_snapshot (ts, state_code);
        """)

        # ---------------- report_table ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_table (
            schedule_minute INT DEFAULT 0 CHECK (schedule_minute >=0 AND schedule_minute <60),
            report_name TEXT PRIMARY KEY,
            configuration JSONB NOT NULL,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            is_scheduled BOOLEAN DEFAULT FALSE,
            email TEXT,
            schedule_time INT CHECK (schedule_time >= 0 AND schedule_time < 24),
            schedule_live INT CHECK (schedule_live IN (7, 30))
        );
        """)

        # ---------------- report_delivery_log ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_delivery_log (
            id SERIAL PRIMARY KEY,
            report_name TEXT REFERENCES report_table(report_name) ON DELETE CASCADE,
            run_date DATE NOT NULL,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'sent',
            error TEXT
        );
        """)

        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS report_delivery_unique_day
            ON report_delivery_log (report_name, run_date);
        """)

        # ================= STRATEGIES TABLE =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # ================= STRATEGIES SEED =================
        cursor.execute("""
        INSERT INTO strategies (id, name, description, enabled)
        VALUES
        (0, 'Manual Override', 'Manual Override BESS control strategy', true),
        (1, 'Capped Dynamic', 'It is a smart ToU BESS strategy uses tariff rate and soc to make decision of charge/discharge', true),
        (2, 'ToU Tariff', 'Flat ToU BESS control strategy', true),
        (3, 'Peak Shaving', 'Peak Shaving BESS control strategy', true),
        (4, 'BASIC', 'It is the BESS Basic Strategy.', true)
        ON CONFLICT (id) DO NOTHING;
        """)

        # ---------------- audit_logs ----------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message TEXT NOT NULL,
                username TEXT NOT NULL,
                ip TEXT NOT NULL
            );
        """)

        # ---------------- security_policies ----------------
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS security_policies (
                role_name TEXT PRIMARY KEY REFERENCES roles(name) ON DELETE CASCADE,
                min_length INT NOT NULL DEFAULT 8,
                min_numeric INT NOT NULL DEFAULT 1,
                min_alpha INT NOT NULL DEFAULT 1,
                min_special INT NOT NULL DEFAULT 1,
                expiry_days INT NOT NULL DEFAULT 60,
                auto_logoff_minutes INT NOT NULL DEFAULT 10,
                lock_after_attempts INT NOT NULL DEFAULT 3,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

         # ---------------- role_permissions (JSONB VERSION) ----------------
        cursor.execute("""
CREATE TABLE IF NOT EXISTS role_permissions (
    role_name TEXT PRIMARY KEY REFERENCES roles(name) ON DELETE CASCADE,
    permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
""")

        # Migrate old columns → JSONB (safe)
        cursor.execute("""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='role_permissions'
        AND column_name='can_edit_user'
    ) THEN

        UPDATE role_permissions
        SET permissions = jsonb_build_object(
            'can_edit_user', can_edit_user,
            'can_delete_user', can_delete_user,
            'can_settings_user', can_settings_user,
            'can_control_bess', can_control_bess,
            'can_control_service', can_control_service,
            'can_control_switchgear', can_control_switchgear,
            'can_control_plant_config', can_control_plant_config
        );

        ALTER TABLE role_permissions DROP COLUMN can_edit_user;
        ALTER TABLE role_permissions DROP COLUMN can_delete_user;
        ALTER TABLE role_permissions DROP COLUMN can_settings_user;
        ALTER TABLE role_permissions DROP COLUMN can_control_bess;
        ALTER TABLE role_permissions DROP COLUMN can_control_service;
        ALTER TABLE role_permissions DROP COLUMN can_control_switchgear;
        ALTER TABLE role_permissions DROP COLUMN can_control_plant_config;

    END IF;
END $$;
""")

        # Seed role_permissions
        cursor.execute("""
            INSERT INTO role_permissions(role_name, permissions)
            VALUES
('admin', '{"can_create_user":true,"can_edit_user":true,"can_delete_user":true,"can_settings_user":true,"can_control_bess":true,"can_control_service":true,"can_control_switchgear":true,"can_control_plant_config":false}'),
('cogneta','{"can_create_user":true,"can_edit_user":true,"can_delete_user":true,"can_settings_user":true,"can_control_bess":true,"can_control_service":true,"can_control_switchgear":true,"can_control_plant_config":true}'),
('manager','{"can_create_user":false,"can_edit_user":true,"can_delete_user":false,"can_settings_user":false,"can_control_bess":true,"can_control_service":true,"can_control_switchgear":true,"can_control_plant_config":false}'),
('supervisor','{"can_create_user":false,"can_edit_user":true,"can_delete_user":false,"can_settings_user":false,"can_control_bess":true,"can_control_service":true,"can_control_switchgear":false,"can_control_plant_config":false}'),
('engineer','{"can_create_user":false,"can_edit_user":false,"can_delete_user":false,"can_settings_user":false,"can_control_bess":true,"can_control_service":true,"can_control_switchgear":false,"can_control_plant_config":false}'),
('operator','{"can_create_user":false,"can_edit_user":false,"can_delete_user":false,"can_settings_user":false,"can_control_bess":false,"can_control_service":false,"can_control_switchgear":false,"can_control_plant_config":false}'),
('guest','{"can_create_user":false,"can_edit_user":false,"can_delete_user":false,"can_settings_user":false,"can_control_bess":false,"can_control_service":false,"can_control_switchgear":false,"can_control_plant_config":false}')
            ON CONFLICT (role_name) DO UPDATE SET
permissions = EXCLUDED.permissions;
        """)

        # Seed security_policies
        cursor.execute("""
            INSERT INTO security_policies(
                role_name, min_length, min_numeric, min_alpha, min_special,
                expiry_days, auto_logoff_minutes, lock_after_attempts
            ) VALUES
              ('admin',10,1,1,1,60,10,3),
              ('manager',9,1,1,1,75,15,3),
              ('supervisor',9,1,1,1,90,15,5),
              ('engineer',8,1,1,1,90,20,5),
              ('operator',8,1,1,1,120,30,5),
              ('guest',8,1,1,1,180,60,5),
              ('cogneta',10,1,1,1,60,10,3)
            ON CONFLICT (role_name) DO NOTHING;
        """)

        # ---------------- dashboards ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dashboards (
          id TEXT PRIMARY KEY,
          title TEXT,
          config JSONB NOT NULL DEFAULT '{}'
        );
        """)

        # ---------------- strategies ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
        """)

        # ---------------- strategy_configs ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_configs (
            id SERIAL PRIMARY KEY,
            strategy_id INTEGER REFERENCES strategies(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            value_text TEXT,
            value_num DOUBLE PRECISION,
            value_bool BOOLEAN,
            schema_type TEXT NOT NULL, -- "boolean", "number", "text", "json"
            description TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            UNIQUE(strategy_id, key)
        );
        """)

        # ---------------- ems_status ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ems_status (
            id SERIAL PRIMARY KEY,
            flag_key TEXT NOT NULL,
            flag_value BOOLEAN NOT NULL DEFAULT FALSE,
            description TEXT,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            UNIQUE(flag_key)
        );
        """)

        # ---------------- setpoint_writes ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS setpoint_writes (
            id SERIAL PRIMARY KEY,
            strategy_id INTEGER REFERENCES strategies(id) ON DELETE SET NULL,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT now(),
            register_mode INTEGER, -- e.g., 0=idle,1=charge,2=discharge
            setpoint_value DOUBLE PRECISION,
            unit TEXT,
            device_id TEXT,
            success BOOLEAN,
            response_text TEXT
        );
        """)

        # ---------------- strategy_execution_logs ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_execution_logs (
            id SERIAL PRIMARY KEY,
            strategy_id INTEGER REFERENCES strategies(id) ON DELETE SET NULL,
            started_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            finished_at TIMESTAMP WITH TIME ZONE,
            input_snapshot JSONB,
            decision_snapshot JSONB,
            error_text TEXT
        );
        """)

        # ---------------- configs ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS configs (
            id SERIAL PRIMARY KEY,

            -- SCOPES:
            -- 'strategy' -> strategy-specific configs (entity_id = strategy.id)
            -- 'bess'     -> global BESS configs (entity_id = NULL)
            -- 'cycle'    -> global cycle rules (entity_id = NULL)
            -- 'system'   -> generic global system configs
            -- 'device'   -> device-level configs (entity_id = device.id)
            -- etc.
            scope TEXT NOT NULL DEFAULT 'system',

            -- Optional foreign key depending on scope
            entity_id INTEGER,

            key TEXT NOT NULL,

            -- Typed values
            value_text TEXT,
            value_num DOUBLE PRECISION,
            value_bool BOOLEAN,
            value_json JSONB,

            value_type TEXT NOT NULL,  -- 'text', 'number', 'boolean', 'json'

            description TEXT,

            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),

            -- Prevent duplicate configs per scope/entity
            UNIQUE(scope, entity_id, key)
        );
        """)

        # ---------------- ems_suggestions ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.ems_suggestions (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            suggestion_type VARCHAR(50),             -- "trading", "dsm", "grid_req", "forecast", etc
            priority VARCHAR(20),                   -- low / medium / high / critical
            title TEXT,                              -- short headline
            description TEXT,                        -- complete suggestion body
            metadata JSONB DEFAULT '{}'::jsonb,      -- flexible structure (SoC, tariff, penalty calc, etc)
            executed BOOLEAN DEFAULT FALSE,          -- if EMS executed it
            executed_at TIMESTAMP WITHOUT TIME ZONE, -- if executed, timestamp
            remarks TEXT
        );
        """)

        #  PRIMARY BACKEND HEALTH
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS primary_backend_health (
            id BIGSERIAL PRIMARY KEY,
            down_at TIMESTAMP WITH TIME ZONE NOT NULL,
            up_at TIMESTAMP WITH TIME ZONE,
            duration_seconds DOUBLE PRECISION,
            status VARCHAR(20) NOT NULL DEFAULT 'DOWN',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # ================= BACKEND SELF HEALTH =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS backend_self_health (
            id BIGSERIAL PRIMARY KEY,
            backend_name VARCHAR(20) NOT NULL,
            status VARCHAR(20) NOT NULL,
            checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # Useful Indexes for ems_suggestions
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ems_suggestions_type ON ems_suggestions (suggestion_type);
        CREATE INDEX IF NOT EXISTS idx_ems_suggestions_ts ON ems_suggestions (ts);
        CREATE INDEX IF NOT EXISTS idx_ems_suggestions_executed ON ems_suggestions (executed);
        """)

        # ---------------- market_bids ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_bids (
            id BIGSERIAL PRIMARY KEY,

            bid_id UUID NOT NULL,
            market VARCHAR(10) NOT NULL,         -- DAM / RTM / TAM
            entity VARCHAR(50) NOT NULL,         -- IEX / PXIL
            asset_id TEXT NOT NULL,              -- BESS / Plant ID

            trade_date DATE NOT NULL,
            block_no INT NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            step_no INT NOT NULL,
            direction VARCHAR(4) NOT NULL CHECK (direction IN ('BUY', 'SELL')),
            quantity_mw NUMERIC(10,4) NOT NULL,
            price_rs_per_kwh NUMERIC(10,4) NOT NULL,

            status VARCHAR(20) NOT NULL CHECK (
                status IN (
                    'PLANNED',
                    'FINALIZED',
                    'APPROVED',
                    'REJECTED',
                    'SUBMITTED',
                    'FAILED',
                    'CLEARED'
                )
            ),

            approval_required BOOLEAN DEFAULT TRUE,
            approved_by TEXT,
            approved_at TIMESTAMP,

            exchange_ack JSONB,
            clearing_result JSONB,

            remarks TEXT,

            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),

            UNIQUE (market, trade_date, block_no, asset_id, step_no)
        );
        """)

        # ---------------- ems_strategy_flags ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ems_strategy_flags (
            id SERIAL PRIMARY KEY,
            strategy_name TEXT NOT NULL,
            flag_name TEXT NOT NULL,
            flag_value INTEGER NOT NULL,
            raised_date DATE NOT NULL,
            raised_at TIMESTAMP NOT NULL DEFAULT NOW(),
            UNIQUE (strategy_name, flag_name, raised_date)
        );
        """)

        # ---------------- telemetry_garbage ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_garbage (
            id BIGSERIAL PRIMARY KEY,

            time_stamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            device_id TEXT NOT NULL,
            device_type TEXT,
            block NUMERIC,

            telemetry_alias TEXT NOT NULL,
            telemetry_name TEXT,

            raw_value NUMERIC,           -- value before scaling (optional)
            scaled_value NUMERIC,        -- value after scaling
            unit TEXT,

            min_value NUMERIC,
            max_value NUMERIC,

            reason TEXT NOT NULL,        -- e.g. 'OUT_OF_RANGE'
            remarks TEXT,                -- human readable reason

            register INTEGER,
            data_type TEXT,

            metadata JSONB               -- extensible (packet id, source, rule version, etc.)
        );
        """)
        
                # ================= ENERGY BLOCK SUMMARY =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_block_summary (
            id BIGSERIAL PRIMARY KEY,
            plant_id TEXT,
            block_date DATE NOT NULL,
            block_number INT CHECK (block_number BETWEEN 1 AND 96),
            block_start TIMESTAMP NOT NULL,
            block_end TIMESTAMP NOT NULL,
            export_kwh NUMERIC(14,4),
            import_kwh NUMERIC(14,4),
            net_export_kwh NUMERIC(14,4),
            deemed_energy_kwh NUMERIC(14,4),
            delivered_energy_kwh NUMERIC(14,4),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (plant_id, block_date, block_number)
        );
        """)

        # ================= ENERGY DAILY SUMMARY =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_daily_summary (
            id BIGSERIAL PRIMARY KEY,
            plant_id TEXT,
            summary_date DATE,
            total_export_kwh NUMERIC(14,4),
            total_import_kwh NUMERIC(14,4),
            total_net_export_kwh NUMERIC(14,4),
            total_deemed_energy_kwh NUMERIC(14,4),
            total_delivered_energy_kwh NUMERIC(14,4),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (plant_id, summary_date)
        );
        """)

        # ================= ENERGY MONTHLY SUMMARY =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_monthly_summary (
            id BIGSERIAL PRIMARY KEY,
            plant_id TEXT,
            year INT,
            month INT,
            total_export_kwh NUMERIC(16,4),
            total_import_kwh NUMERIC(16,4),
            total_net_export_kwh NUMERIC(16,4),
            total_deemed_energy_kwh NUMERIC(16,4),
            total_delivered_energy_kwh NUMERIC(16,4),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (plant_id, year, month)
        );
        """)

        # ================= GRID REQUEST =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_req (
            id SERIAL PRIMARY KEY,
            req_date DATE NOT NULL,
            start_ts TIMESTAMP NOT NULL,
            end_ts TIMESTAMP NOT NULL,
            type VARCHAR(50),
            power NUMERIC(10,2),
            unit VARCHAR(10) DEFAULT 'kW',
            remark TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # ================= DSM BLOCK SUMMARY =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dsm_block_summary (
            id BIGSERIAL PRIMARY KEY,
            plant_id TEXT,
            block_time TIMESTAMP NOT NULL,
            block_no INT NOT NULL,
            scheduled_power_kw NUMERIC(16,4),
            scheduled_energy_kwh NUMERIC(16,4),
            actual_export_kwh NUMERIC(16,4),
            actual_import_kwh NUMERIC(16,4),
            net_export_kwh NUMERIC(16,4),
            deemed_energy_kwh NUMERIC(16,4),
            delivered_energy_kwh NUMERIC(16,4),
            deviation_kwh NUMERIC(16,4),
            frequency NUMERIC,
            dsm_rate NUMERIC,
            dsm_charge NUMERIC(16,4),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (plant_id, block_time, block_no)
        );
        """)

        # ================= DSM DAILY SUMMARY =================
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dsm_daily_summary (
            id BIGSERIAL PRIMARY KEY,
            plant_id TEXT,
            summary_date DATE,
            total_scheduled_energy_kwh NUMERIC(16,4),
            total_actual_energy_kwh NUMERIC(16,4),
            total_deemed_energy_kwh NUMERIC(16,4),
            total_delivered_energy_kwh NUMERIC(16,4),
            total_deviation_kwh NUMERIC(16,4),
            total_dsm_charge NUMERIC(16,4),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (plant_id, summary_date)
        );
        """)

        # ---------------- widget_templates ----------------
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS widget_templates (
            id UUID PRIMARY KEY,
            name TEXT,
            type TEXT,
            title TEXT,
            config JSONB,
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # Insert default widget templates
        cursor.execute("""
        INSERT INTO public.widget_templates
        (id, name, type, title, config, description, is_active)
        VALUES

        (
        '4fd525bb-8cec-4f60-a9e3-2edc5ad13649',
        'advance_table',
        'advance_table',
        'Advance Table',
        $$
        {
          "type": "advance_table",
          "props": {"height": "250px","chartProps": {}},
          "table": {
            "columns": [
              {"key": "device","label": "Device"},
              {"key": "status","label": "Status"},
              {"key": "soc","label": "SoC (%)"},
              {"key": "voltage","label": "Voltage (V)"}
            ],
            "pageSize": 20
          },
          "title": "Advance Table",
          "dataSource": {
            "keys": [],
            "devices": {"ids": [],"patterns": []},
            "execute": "fn_get_latest_timeseries_data",
            "orderBy": "DESC",
            "timeRange": {
              "type": "Relative",
              "preset": "Today So Far",
              "interval": 1,
              "intervalType": "day"
            },
            "isRealTime": true,
            "aggregation": {"func": "AVG","interval": 15,"intervalType": "minutes"},
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "advanceTableData"
          },
          "exportEnabled": true
        }
        $$::jsonb,
        'Advance table (keys & devices blank)',
        true
        ),

        (
        '53250992-40cd-4c15-ae88-f96dab02baca',
        'combination_graph',
        'graph',
        'Combination Graph',
        $$
        {
          "graph": {"type": ""},
          "props": {
            "chartProps": {
              "chart": {
                "style": {"fontFamily": "Montserrat, sans-serif"},
                "zoomType": "x"
              },
              "title": {"text": ""},
              "xAxis": {"type": "datetime"},
              "yAxis": [{"title": {"text": ""}}],
              "legend": {"enabled": true},
              "tooltip": {"shared": true}
            },
            "widgetProps": {}
          },
          "dataSource": {
            "keys": [],
            "devices": {"ids": [],"patterns": []},
            "execute": "fn_get_timeseries_data",
            "orderBy": "ASC",
            "timeRange": {"type": "last","interval": 4,"intervalType": "hour"},
            "isRealTime": true,
            "aggregation": {"func": "avg","interval": 5,"intervalType": "minutes"},
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "essPowerData"
          },
          "exportEnabled": true
        }
        $$::jsonb,
        'Combination graph (keys & patterns blank)',
        true
        ),

        (
        '61edfea0-51c8-4fe0-8544-2bf25d13c77e',
        'advance_history_table',
        'advance_table',
        'advance_History_table',
        $$
        {
          "type": "advance_table",
          "graph": {},
          "props": {"height": "250px","chartProps": {}},
          "table": {
            "mode": "history",
            "columns": [
              {"key": "device","label": "Device"},
              {"key": "ts","label": "Timestamp"},
              {"key": "soc","label": "SoC (%)"},
              {"key": "voltage","label": "ESS(V)"}
            ],
            "pageSize": 100
          },
          "title": "advance_History_table",
          "dataSource": {
            "keys": [],
            "devices": {"ids": []},
            "execute": "fn_get_timeseries_data",
            "orderBy": "ASC",
            "timeRange": {"type": "last","interval": 1,"intervalType": "day"},
            "isRealTime": true,
            "aggregation": {"func": "avg","interval": 5,"intervalType": "minutes"},
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "HistoryTableData"
          },
          "exportEnabled": true
        }
        $$::jsonb,
        'Advance history table (keys & devices blank)',
        true
        ),

        (
        '7efc35cc-52b8-4eaa-9394-5eaa4af27d7e',
        'simple_history_table',
        'table',
        'Advance History Table',
        $$
        {
          "type": "table",
          "graph": {},
          "props": {"height": "250px","chartProps": {}},
          "table": {
            "mode": "history",
            "columns": [
              {"key": "device","label": "Device"},
              {"key": "ts","label": "Timestamp"},
              {"key": "soc","label": "SoC (%)"}
            ],
            "pageSize": 100
          },
          "title": "Advance History Table",
          "dataSource": {
            "keys": [],
            "devices": {"ids": []},
            "execute": "fn_get_timeseries_data",
            "orderBy": "ASC",
            "timeRange": {"type": "last","interval": 1,"intervalType": "day"},
            "isRealTime": true,
            "aggregation": {"func": "avg","interval": 5,"intervalType": "minutes"},
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "HistoryTableData"
          },
          "exportEnabled": true
        }
        $$::jsonb,
        'Simple history table',
        true
        ),

        (
        '8cc80460-e470-44c8-8e03-acb57686ebeb',
        'simple_latest_table',
        'table',
        'Simple Latest Table',
        $$
        {
          "type": "table",
          "props": {},
          "table": {
            "columns": [
              {"key": "device","label": "Device"},
              {"key": "status","label": "Status"},
              {"key": "soc","label": "SoC (%)"}
            ],
            "pageSize": 20
          },
          "title": "Simple Latest Table",
          "dataSource": {
            "keys": [],
            "devices": {"ids": [],"patterns": []},
            "execute": "fn_get_latest_timeseries_data",
            "isRealTime": true,
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "LatestTableData"
          },
          "exportEnabled": true
        }
        $$::jsonb,
        'Simple latest table',
        true
        ),

        (
        '9c3d2e41-6b7f-4d9c-bf8a-3c2a8e9f5a61',
        'soc_solid_gauge',
        'latest_card',
        'SOC Solid Gauge',
        $$
        {
          "props": {},
          "standard": {"componentName": "SolidGauge"},
          "dataSource": {
            "keys": [],
            "devices": {"ids": [],"patterns": []},
            "execute": "fn_get_latest_timeseries_data",
            "isRealTime": true,
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "totalSoCPowerData"
          },
          "solid_gauge": {
            "componentProps": {
              "width": 380,
              "colors": ["#12D622","#A1E453","#FFD739","#FF5223","#FF2929"],
              "height": 260,
              "segments": 5
            }
          },
          "exportEnabled": false
        }
        $$::jsonb,
        'SOC solid gauge',
        true
        ),

        (
        'b53e722a-eda5-4c6b-bc9d-7e046723d301',
        'circular_progress',
        'latest_card',
        'Circular Progress',
        $$
        {
          "props": {},
          "standard": {"componentName": "SolidGauge"},
          "dataSource": {
            "keys": [],
            "devices": {"ids": [],"patterns": []},
            "execute": "fn_get_latest_timeseries_data",
            "isRealTime": true,
            "executorType": "sql_function",
            "pollInterval": 10000,
            "returnDataName": "totalSoCPowerData"
          },
          "solid_gauge": {
            "componentProps": {
              "width": 380,
              "colors": ["#12D622","#A1E453","#FFD739","#FF5223","#FF2929"],
              "height": 260,
              "segments": 5
            }
          },
          "exportEnabled": false
        }
        $$::jsonb,
        'Circular progress meter',
        true
        )

        ON CONFLICT (id) DO NOTHING;
        """)

        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_garbage_device_time
        ON telemetry_garbage (device_id, time_stamp DESC);

        CREATE INDEX IF NOT EXISTS idx_garbage_alias_time
        ON telemetry_garbage (telemetry_alias, time_stamp DESC);

        CREATE INDEX IF NOT EXISTS idx_garbage_reason
        ON telemetry_garbage (reason);

        CREATE INDEX IF NOT EXISTS idx_garbage_metadata
        ON telemetry_garbage USING GIN (metadata);
        """)

        cursor.execute("""
        CREATE OR REPLACE FUNCTION public.fn_get_timeseries_data(
            widget_json jsonb
        )
        RETURNS TABLE(
            time_stamp timestamp without time zone,
            key text,
            device_id text,
            value double precision
        )
        LANGUAGE 'plpgsql'
        COST 100
        VOLATILE
        PARALLEL UNSAFE
        ROWS 1000

        AS $BODY$

        DECLARE
            device_id TEXT;
            key_info JSONB;
            key_name TEXT;
            alias TEXT;
            key_type TEXT;
            agg_func TEXT;
            interval_val INT;
            interval_type TEXT;
            start_ts BIGINT;
            end_ts BIGINT;
            dynamic_sql TEXT := '';
            device_index INT;
            time_type TEXT;
            rel_type TEXT;
            last_interval INT;
            last_type TEXT;
            value_expr TEXT;
            group_clause TEXT;
            order_by TEXT;
            interval_in_seconds INT;
            local_tz TEXT := 'Asia/Kolkata';

        BEGIN

            -- Aggregation read
            agg_func := widget_json->'dataSource'->'aggregation'->>'func';
            interval_val := (widget_json->'dataSource'->'aggregation'->>'interval')::INT;
            interval_type := widget_json->'dataSource'->'aggregation'->>'intervalType';

            -- Interval seconds calculation
            interval_in_seconds :=
                CASE lower(interval_type)
                    WHEN 'second'  THEN interval_val
                    WHEN 'seconds' THEN interval_val
                    WHEN 'minute'  THEN interval_val * 60
                    WHEN 'minutes' THEN interval_val * 60
                    WHEN 'min'     THEN interval_val * 60
                    WHEN 'mins'    THEN interval_val * 60
                    WHEN 'hour'    THEN interval_val * 3600
                    WHEN 'hours'   THEN interval_val * 3600
                    WHEN 'day'     THEN interval_val * 86400
                    WHEN 'days'    THEN interval_val * 86400
                    WHEN 'week'    THEN interval_val * 7 * 86400
                    WHEN 'weeks'   THEN interval_val * 7 * 86400
                    ELSE NULL
                END;

            IF interval_in_seconds IS NULL THEN
                RAISE EXCEPTION
                'Unsupported interval type "%" for custom intervals. Only second/minute/hour/day/week are supported with any interval value.',
                interval_type;
            END IF;

            -- Allowed aggregation
            IF NOT (lower(agg_func) = ANY (ARRAY['avg','sum','min','max','count','none','last'])) THEN
                RAISE EXCEPTION 'Unsupported aggregation function: %', agg_func;
            END IF;

            -- Time range handling
            time_type := widget_json->'dataSource'->'timeRange'->>'type';

            IF time_type = 'absolute' THEN

                start_ts := (widget_json->'dataSource'->'timeRange'->>'startTs')::BIGINT / 1000;
                end_ts   := (widget_json->'dataSource'->'timeRange'->>'endTs')::BIGINT / 1000;

            ELSIF time_type = 'relative' THEN

                rel_type := widget_json->'dataSource'->'timeRange'->>'relativeType';

                IF rel_type = 'currentDaySoFar' THEN
                    start_ts := extract(epoch FROM date_trunc('day', now()));
                    end_ts   := extract(epoch FROM now());

                ELSIF rel_type = 'yesterday' THEN
                    start_ts := extract(epoch FROM date_trunc('day', now() - interval '1 day'));
                    end_ts   := extract(epoch FROM date_trunc('day', now()));

                ELSIF rel_type = 'currentWeekSoFar' THEN
                    start_ts := extract(epoch FROM date_trunc('week', now()));
                    end_ts   := extract(epoch FROM now());

                ELSIF rel_type = 'currentMonthSoFar' THEN
                    start_ts := extract(epoch FROM date_trunc('month', now()));
                    end_ts   := extract(epoch FROM now());

                ELSIF rel_type = 'currentQuarterSoFar' THEN
                    start_ts := extract(epoch FROM date_trunc('quarter', now()));
                    end_ts   := extract(epoch FROM now());

                ELSE
                    RAISE EXCEPTION 'Unknown relative type: %', rel_type;
                END IF;

            ELSIF time_type = 'last' THEN

                last_interval := (widget_json->'dataSource'->'timeRange'->>'interval')::INT;
                last_type     := widget_json->'dataSource'->'timeRange'->>'intervalType';

                start_ts := extract(
                    epoch FROM (
                        now() - make_interval(
                            hours  => CASE WHEN lower(last_type) IN ('hours','hour') THEN last_interval ELSE 0 END,
                            days   => CASE WHEN lower(last_type) IN ('days','day') THEN last_interval ELSE 0 END,
                            mins   => CASE WHEN lower(last_type) IN ('minutes','minute','mins','min') THEN last_interval ELSE 0 END,
                            weeks  => CASE WHEN lower(last_type) IN ('weeks','week') THEN last_interval ELSE 0 END,
                            months => CASE WHEN lower(last_type) IN ('months','month') THEN last_interval ELSE 0 END,
                            years  => CASE WHEN lower(last_type) IN ('years','year') THEN last_interval ELSE 0 END
                        )
                    )
                );

                end_ts := extract(epoch FROM now());

            ELSE
                RAISE EXCEPTION 'Unknown time range type: %', time_type;
            END IF;

            -- Order By
            order_by := widget_json->'dataSource'->>'orderBy';

            IF upper(order_by) NOT IN ('ASC','DESC') THEN
                RAISE EXCEPTION 'Invalid ORDER BY direction: must be ASC or DESC';
            END IF;

            -- Temp table
            DROP TABLE IF EXISTS temp_result;

            CREATE TEMP TABLE temp_result (
                time_stamp TIMESTAMP,
                key TEXT,
                device_id TEXT,
                value FLOAT8
            ) ON COMMIT DROP;

            -- Loop devices
            FOR device_index IN 1 .. jsonb_array_length(widget_json->'dataSource'->'devices'->'ids')
            LOOP

                device_id := widget_json->'dataSource'->'devices'->'ids'->>(device_index - 1);

                -- Loop keys
                FOR key_info IN
                    SELECT * FROM jsonb_array_elements(widget_json->'dataSource'->'keys')
                LOOP

                    key_name := key_info->>'key';
                    alias    := key_info->>'alias';
                    key_type := key_info->>'keyType';

                    PERFORM 1
                    FROM information_schema.columns
                    WHERE table_name = device_id
                      AND column_name = key_name;

                    IF NOT FOUND THEN
                        RAISE NOTICE 'Skipping key "%" for device "%" because column does not exist',
                        key_name, device_id;
                        CONTINUE;
                    END IF;

                                        -- LAST aggregation
                    IF lower(agg_func) = 'last' THEN

                        dynamic_sql := format($sql$
                            INSERT INTO temp_result (time_stamp, key, device_id, value)
                            SELECT
                                date_trunc('day', time_stamp AT TIME ZONE %L)::timestamp,
                                %L,
                                %L,
                                (ARRAY_AGG((%I)::float8 ORDER BY time_stamp AT TIME ZONE %L DESC))[1]
                            FROM %I
                            WHERE (time_stamp AT TIME ZONE %L)::timestamptz
                                BETWEEN to_timestamp(%L) AND to_timestamp(%L)
                            GROUP BY 1
                        $sql$,
                        local_tz,
                        alias,
                        device_id,
                        key_name,
                        local_tz,
                        device_id,
                        local_tz,
                        start_ts,
                        end_ts
                        );

                    ELSIF lower(agg_func) = 'none' THEN

                        dynamic_sql := format($sql$
                            INSERT INTO temp_result (time_stamp, key, device_id, value)
                            SELECT
                                time_stamp,
                                %L,
                                %L,
                                (%I)::float8
                            FROM %I
                            WHERE (time_stamp AT TIME ZONE %L)::timestamptz
                                BETWEEN to_timestamp(%L) AND to_timestamp(%L)
                            ORDER BY time_stamp
                        $sql$,
                        alias,
                        device_id,
                        key_name,
                        device_id,
                        local_tz,
                        start_ts,
                        end_ts
                        );

                    ELSE

                        dynamic_sql := format($sql$
                            INSERT INTO temp_result (time_stamp, key, device_id, value)
                            SELECT
                                to_timestamp(
                                    floor(
                                        extract(epoch FROM (time_stamp AT TIME ZONE %L)::timestamptz) / %s
                                    ) * %s
                                ),
                                %L,
                                %L,
                                %s((%I)::float8)
                            FROM %I
                            WHERE (time_stamp AT TIME ZONE %L)::timestamptz
                                BETWEEN to_timestamp(%L) AND to_timestamp(%L)
                            GROUP BY 1
                        $sql$,
                        local_tz,
                        interval_in_seconds,
                        interval_in_seconds,
                        alias,
                        device_id,
                        agg_func,
                        key_name,
                        device_id,
                        local_tz,
                        start_ts,
                        end_ts
                        );

                    END IF;

                    EXECUTE dynamic_sql;

                END LOOP;

            END LOOP;

            RETURN QUERY EXECUTE
                'SELECT time_stamp, key, device_id, value
                 FROM temp_result
                 ORDER BY time_stamp ' || upper(order_by) || ', device_id, key';

        END;

        $BODY$;
        """)

        # FUNCTION 2 → fn_get_latest_timeseries_data
        cursor.execute("""
        CREATE OR REPLACE FUNCTION public.fn_get_latest_timeseries_data(
        	widget_json jsonb)
        RETURNS TABLE(
        	time_stamp timestamp without time zone,
        	key text,
        	device_id text,
        	value double precision
        )
        LANGUAGE plpgsql
        AS $BODY$
        DECLARE
            device_id TEXT;
            key_info JSONB;
            key_name TEXT;
            alias TEXT;
            device_index INT;
            dynamic_sql TEXT;
        BEGIN

            EXECUTE 'DROP TABLE IF EXISTS temp_result';
            EXECUTE 'CREATE TEMP TABLE temp_result (
                time_stamp TIMESTAMP,
                key TEXT,
                device_id TEXT,
                value FLOAT8
            ) ON COMMIT DROP';

            FOR device_index IN 1 .. jsonb_array_length(widget_json->'dataSource'->'devices'->'ids')
            LOOP
                device_id := widget_json->'dataSource'->'devices'->'ids'->>(device_index - 1);

                FOR key_info IN SELECT * FROM jsonb_array_elements(widget_json->'dataSource'->'keys')
                LOOP
                    key_name := key_info->>'key';
                    alias := key_info->>'alias';

                    PERFORM 1
                    FROM information_schema.columns
                    WHERE table_name = device_id
                      AND column_name = key_name;

                    IF NOT FOUND THEN
                        CONTINUE;
                    END IF;

                    dynamic_sql := format($sql$
                        INSERT INTO temp_result (time_stamp, key, device_id, value)
                        SELECT 
                            time_stamp,
                            %L,
                            %L,
                            (%I)::float8
                        FROM %I
                        ORDER BY time_stamp DESC
                        LIMIT 1
                    $sql$,
                        key_name,
                        device_id,
                        key_name,
                        device_id
                    );

                    EXECUTE dynamic_sql;
                END LOOP;
            END LOOP;

            RETURN QUERY
            SELECT * FROM temp_result
            ORDER BY device_id, key;

        END;
        $BODY$;
        """)

        conn.commit()
        print("Application schema created successfully.")

    except Exception as e:
        conn.rollback()
        print(" Error creating schema:", e)

    finally:
        cursor.close()
        conn.close()
