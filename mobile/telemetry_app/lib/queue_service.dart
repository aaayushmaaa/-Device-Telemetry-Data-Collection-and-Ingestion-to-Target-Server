import 'package:sqflite/sqflite.dart';
import 'package:path/path.dart';

class QueueService {
  static Database? _db;

  Future<Database> get database async {
    if (_db != null) return _db!;
    _db = await initDB();
    return _db!;
  }

  initDB() async {
    final path = join(await getDatabasesPath(), "telemetry.db");
    return await openDatabase(
      path,
      version: 1,
      onCreate: (db, _) async {
        await db.execute("""
          CREATE TABLE queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
          );
        """);
      },
    );
  }

  Future<void> add(String payload) async {
    final db = await database;
    await db.insert("queue", {"payload": payload});
  }

  Future<List<Map<String, dynamic>>> fetchAll() async {
    final db = await database;
    return db.query("queue");
  }

  Future<void> remove(int id) async {
    final db = await database;
    await db.delete("queue", where: "id=?", whereArgs: [id]);
  }
}
