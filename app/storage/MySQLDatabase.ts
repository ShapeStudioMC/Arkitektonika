import mysql from 'mysql2/promise';
import Logger from '../Logger.js';
import IDataStorage from './IDataStorage.js';
import { SchematicRecord } from '../model/SchematicRecord.js';
import { customAlphabet } from 'nanoid';

const ID_GENERATOR = customAlphabet('0123456789abcdef', 32);

export default class MySQLDatabase implements IDataStorage {
    private readonly pool: mysql.Pool;
    private readonly logger: Logger;

    constructor(logger: Logger) {
        this.logger = logger;
        this.pool = mysql.createPool({
            host: process.env.MYSQL_HOST || 'localhost',
            user: process.env.MYSQL_USER || 'root',
            password: process.env.MYSQL_PASSWORD || '',
            database: process.env.MYSQL_DATABASE || 'arkitektonika',
            waitForConnections: true,
            connectionLimit: 5,
            queueLimit: 0,
        });
        this.migrate().then(() => logger.debug('MySQL migration completed.'));
    }

    async getAllRecords(): Promise<SchematicRecord[]> {
        const [rows] = await this.pool.query('SELECT * FROM accounting');
        return (rows as any[]).map(MySQLDatabase.transformRowToRecord);
    }

    async getAllUnexpiredRecords(): Promise<SchematicRecord[]> {
        const [rows] = await this.pool.query('SELECT * FROM accounting WHERE expired IS NULL');
        return (rows as any[]).map(MySQLDatabase.transformRowToRecord);
    }

    async getSchematicRecordByDeleteKey(deleteKey: string): Promise<SchematicRecord> {
        const [rows] = await this.pool.query('SELECT * FROM accounting WHERE delete_key = ? LIMIT 1', [deleteKey]);
        const result = (rows as any[])[0];
        if (!result) throw new Error('No data found for passed delete key');
        return MySQLDatabase.transformRowToRecord(result);
    }

    async getSchematicRecordByDownloadKey(downloadKey: string): Promise<SchematicRecord> {
        const [rows] = await this.pool.query('SELECT * FROM accounting WHERE download_key = ? LIMIT 1', [downloadKey]);
        const result = (rows as any[])[0];
        if (!result) throw new Error('No data found for passed download key');
        return MySQLDatabase.transformRowToRecord(result);
    }

    async expireSchematicRecord(recordId: number): Promise<void> {
        const [result]: any = await this.pool.query('UPDATE accounting SET expired = ? WHERE id = ?', [
            Date.now(),
            recordId,
        ]);
        if (result.affectedRows < 1) throw new Error('Failed to expire schematic - No schematic exists with passed id');
    }

    async storeSchematicRecord(record: SchematicRecord): Promise<SchematicRecord> {
        await this.pool.query(
            'INSERT INTO accounting (filename, download_key, delete_key, last_accessed, uploaded_by, schem_type, pos1, pos2) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [record.fileName, record.downloadKey, record.deleteKey, Date.now(), record.uploader, record.schem_type, record.pos1, record.pos2]
        );
        return record;
    }

    async expireSchematicRecords(milliseconds: number): Promise<SchematicRecord[]> {
        const cutoff = Date.now() - milliseconds;
        const [rows] = await this.pool.query(
            'SELECT * FROM accounting WHERE last_accessed <= ? AND expired IS NULL',
            [cutoff]
        );
        const records: SchematicRecord[] = (rows as any[]).map(MySQLDatabase.transformRowToRecord);
        if (records.length === 0) return [];

        for (const record of records) {
            await this.pool.query('UPDATE accounting SET expired = ? WHERE id = ?', [Date.now(), record.id]);
        }

        return records;
    }

    async generateDeletionKey(maxIterations: number): Promise<string> {
        return this.generateUniqueKey('delete_key', maxIterations);
    }

    async generateDownloadKey(maxIterations: number): Promise<string> {
        return this.generateUniqueKey('download_key', maxIterations);
    }

    private async generateUniqueKey(column: 'delete_key' | 'download_key', maxIterations: number): Promise<string> {
        let iterations = 0;
        while (iterations++ < maxIterations) {
            const key = ID_GENERATOR();
            const [rows] = await this.pool.query(`SELECT id FROM accounting WHERE ${column} = ? LIMIT 1`, [key]);
            if ((rows as any[]).length === 0) {
                return key;
            }
        }
        throw new Error('Failed to generate unique key');
    }

    /**
     * Run initial table creation and indexing.
     */
    private async migrate(): Promise<void> {
        const queries = [
            `CREATE TABLE IF NOT EXISTS accounting (
                id INT AUTO_INCREMENT PRIMARY KEY,
                download_key CHAR(32) NOT NULL UNIQUE,
                delete_key CHAR(32) NOT NULL UNIQUE,
                filename VARCHAR(255) NOT NULL,
                last_accessed BIGINT NOT NULL,
                expired BIGINT NULL,
                uploaded_by CHAR(36) NULL,
                schem_type VARCHAR(10) NULL,
                pos1 VARCHAR(35) NULL,
                pos2 VARCHAR(35) NULL
            );`,
        ];

        for (const query of queries) {
            await this.pool.query(query);
        }
    }

    /**
     * Helper to map MySQL rows to SchematicRecord objects.
     */
    private static transformRowToRecord(row: any): SchematicRecord {
        return {
            id: row.id,
            downloadKey: row.download_key,
            deleteKey: row.delete_key,
            fileName: row.filename,
            expired: row.expired ? new Date(Number(row.expired)) : undefined,
            last_accessed: row.last_accessed ? new Date(Number(row.last_accessed)) : undefined,
            uploader: row.uploaded_by,
            schem_type: row.schem_type,
            pos1: row.pos1,
            pos2: row.pos1
        };
    }
}
