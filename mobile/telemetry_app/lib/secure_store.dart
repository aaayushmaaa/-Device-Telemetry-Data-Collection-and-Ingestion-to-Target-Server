import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'dart:convert';
import 'package:crypto/crypto.dart';

class SecureStore {
  final _storage = const FlutterSecureStorage();

  Future<String> getTokenized(String input) async {
    final deviceKey = await _storage.read(key: "deviceKey");

    if (deviceKey == null) {
      final newKey = DateTime.now().millisecondsSinceEpoch.toString();
      await _storage.write(key: "deviceKey", value: newKey);
      return sha256.convert(utf8.encode("$newKey-$input")).toString();
    }

    return sha256.convert(utf8.encode("$deviceKey-$input")).toString();
  }
}
