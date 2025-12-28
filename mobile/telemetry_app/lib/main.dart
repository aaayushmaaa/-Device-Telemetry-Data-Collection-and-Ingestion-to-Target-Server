import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:device_info_plus/device_info_plus.dart';
import 'package:battery_plus/battery_plus.dart';
import 'package:network_info_plus/network_info_plus.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:sensors_plus/sensors_plus.dart';
import 'package:package_info_plus/package_info_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:uuid/uuid.dart';
import 'package:http/http.dart' as http;
import 'package:archive/archive.dart';


// -----------------------------------------------------
// Allow self-signed HTTPS (DEV ONLY)
// -----------------------------------------------------
class MyHttpOverrides extends HttpOverrides {
  @override
  HttpClient createHttpClient(SecurityContext? context) {
    return super.createHttpClient(context)
      ..badCertificateCallback =
          (X509Certificate cert, String host, int port) => true;
  }
}

void main() {
  HttpOverrides.global = MyHttpOverrides();
  runApp(const MyApp());
}


// -----------------------------------------------------
// App root
// -----------------------------------------------------
class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Telemetry App',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(primarySwatch: Colors.blue),
      home: const TelemetryPage(),
    );
  }
}


// -----------------------------------------------------
// Telemetry Screen
// -----------------------------------------------------
class TelemetryPage extends StatefulWidget {
  const TelemetryPage({super.key});

  @override
  State<TelemetryPage> createState() => _TelemetryPageState();
}

class _TelemetryPageState extends State<TelemetryPage> {
  String _status = "Idle";

  // ---------------------------------------------------
  // Collect + Send Telemetry + Trigger Airflow
  // ---------------------------------------------------
  Future<void> _collectTelemetry() async {
    setState(() => _status = "Collecting telemetry...");

    final deviceInfo = DeviceInfoPlugin();
    final battery = Battery();
    final info = NetworkInfo();
    final packageInfo = await PackageInfo.fromPlatform();
    final sessionId = const Uuid().v4();

    final Map<String, dynamic> telemetry = {};

    try {
      // --------------------
      // Device info
      // --------------------
      final androidInfo = await deviceInfo.androidInfo;
      telemetry['device'] = {
        "model": androidInfo.model,
        "manufacturer": androidInfo.manufacturer,
        "sdk": androidInfo.version.sdkInt,
      };

      // --------------------
      // Battery
      // --------------------
      telemetry['battery'] = {
        "level": await battery.batteryLevel,
      };

      // --------------------
      // Network
      // --------------------
      final connectivity = await Connectivity().checkConnectivity();
      telemetry['network'] = {
        "wifiName": await info.getWifiName(),
        "wifiIP": await info.getWifiIP(),
        "connectivity": connectivity.toString().split('.').last,
      };

      // --------------------
      // App info
      // --------------------
      telemetry['app'] = {
        "name": packageInfo.appName,
        "version": packageInfo.version,
        "buildNumber": packageInfo.buildNumber,
      };

      // --------------------
      // Sensor (single sample)
      // --------------------
      telemetry['sensor'] = {};
      await accelerometerEvents.take(1).forEach((event) {
        telemetry['sensor'] = {
          "accelerometer": {
            "x": event.x,
            "y": event.y,
            "z": event.z,
          }
        };
      });

      // --------------------
      // Session
      // --------------------
      telemetry['session'] = {
        "id": sessionId,
        "timestamp": DateTime.now().toIso8601String(),
      };

      // --------------------
      // Save locally
      // --------------------
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(
        'latest_telemetry',
        jsonEncode(telemetry),
      );

      setState(() => _status = "Telemetry collected ✔");

      // ------------------------------------------------
      // Send telemetry to ingestion API
      // ------------------------------------------------
      setState(() => _status += "\nSending telemetry...");

      final ingestUri =
          Uri.parse("https://10.13.162.170:8000/ingest");

      final jsonData = jsonEncode(telemetry);
      final compressedData =
          GZipEncoder().encode(utf8.encode(jsonData));

      final ingestResponse = await http.post(
        ingestUri,
        headers: {
          "Content-Type": "application/json",
          "x-api-key": "test123",
        },
        body: compressedData,
      );

      if (ingestResponse.statusCode != 200) {
        throw Exception(
            "Ingest failed (${ingestResponse.statusCode})");
      }

      setState(() => _status += "\nTelemetry sent ✔");

      // ------------------------------------------------
      // Trigger Airflow pipeline
      // ------------------------------------------------
      setState(() => _status += "\nTriggering pipeline...");

      final triggerUri =
          Uri.parse("https://10.13.162.170:8000/start-telemetry");

      final triggerResponse = await http.post(triggerUri);

      if (triggerResponse.statusCode != 200) {
        throw Exception(
            "Pipeline trigger failed (${triggerResponse.statusCode})");
      }

      setState(() => _status += "\nPipeline started 🚀");

    } catch (e) {
      setState(() => _status = "❌ Error: $e");
    }
  }


  // ---------------------------------------------------
  // Show saved telemetry
  // ---------------------------------------------------
  Future<void> _showSavedTelemetry() async {
    final prefs = await SharedPreferences.getInstance();
    final telemetry = prefs.getString('latest_telemetry');

    setState(() {
      if (telemetry == null) {
        _status = "No telemetry saved.";
      } else {
        _status = const JsonEncoder.withIndent('  ')
            .convert(jsonDecode(telemetry));
      }
    });
  }


  // ---------------------------------------------------
  // UI
  // ---------------------------------------------------
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text("Telemetry App")),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            Row(
              children: [
                Expanded(
                  child: ElevatedButton(
                    onPressed: _collectTelemetry,
                    child: const Text("Start Telemetry"),
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: ElevatedButton(
                    onPressed: _showSavedTelemetry,
                    child: const Text("Show Saved Telemetry"),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 20),
            Expanded(
              child: SingleChildScrollView(
                child: Text(
                  _status,
                  style: const TextStyle(fontSize: 14),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
