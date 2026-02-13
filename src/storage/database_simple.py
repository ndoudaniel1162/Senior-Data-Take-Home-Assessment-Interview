def insert_telemetry(self, rec: dict):
    sql = """
    INSERT OR IGNORE INTO telemetry (
        event_id,
        device_id,
        timestamp,
        temperature,
        humidity,
        schema_version
    ) VALUES (?, ?, ?, ?, ?, ?)
    """

    self.conn.execute(sql, (
        rec["event_id"],
        rec["device_id"],
        rec["timestamp"],
        rec.get("temperature"),
        rec.get("humidity"),
        rec.get("schema_version"),
    ))

    self.conn.commit()