final prefs = await SharedPreferences.getInstance();
String? telemetry = prefs.getString('latest_telemetry');
print(telemetry); // Should print JSON collected earlier
