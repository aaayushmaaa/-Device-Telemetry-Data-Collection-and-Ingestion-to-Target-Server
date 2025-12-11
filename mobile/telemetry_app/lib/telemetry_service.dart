import 'package:device_info_plus/device_info_plus.dart';
import 'package:battery_plus/battery_plus.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:package_info_plus/package_info_plus.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';

class TelemetryService {
  final deviceInfo = DeviceInfoPlugin();
  final battery = Battery();

  Future<Map<String, dynamic>> collectTelemetry() async {
    // Device info
    AndroidDeviceInfo android = await deviceInfo.androidInfo;

    // Battery info
    int batteryLevel = await battery.batteryLevel;

    // Connectivity
    ConnectivityResult connectivity = await Connectivity().checkConnectivity();

    // App info
    PackageInfo info = await PackageInfo.fromPlatform();

    return {
      "device": {
        "model": android.model,
        "manufacturer": android.manufacturer,
        "androidVersion": android.version.release,
      },
      "battery": {"level": batteryLevel},
      "network": {"type": connectivity.toString()},
      "app": {"version": info.version, "buildNumber": info.buildNumber},
      "timestamp": DateTime.now().toIso8601String(),
    };
  }

  Future<void> sendTelemetry(Map<String, dynamic> data) async {
    final response = await http.post(
      Uri.parse("http://localhost:8000/ingest"), // replace later with API
      headers: {"Content-Type": "application/json"},
      body: json.encode(data),
    );

    print("Server response: ${response.statusCode}");
  }
}
