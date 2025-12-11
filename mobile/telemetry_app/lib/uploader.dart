import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:archive/archive.dart';
import 'queue_service.dart';

class Uploader {
  final queue = QueueService();

  Future<void> uploadPending() async {
    final items = await queue.fetchAll();

    for (final row in items) {
      final id = row["id"];
      final payload = utf8.encode(row["payload"]);

      final gzipData = GZipEncoder().encode(payload);

      final response = await http.post(
        Uri.parse("http://YOUR_FASTAPI_IP:8000/ingest"),
        headers: {
          "Content-Encoding": "gzip",
          "Content-Type": "application/json",
          "X-API-KEY": "test123",
        },
        body: gzipData,
      );

      if (response.statusCode == 200) {
        await queue.remove(id);
      }
    }
  }
}
