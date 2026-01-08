import os from "os";
import { execSync } from "child_process";

function getPrimaryIP() {
  const interfaces = os.networkInterfaces();

  for (const name in interfaces) {
    for (const net of interfaces[name]) {
      if (net.family === "IPv4" && !net.internal) {
        return net.address;
      }
    }
  }

  return null;
}
const args = process.argv.slice(2);
if (args.includes("--stop")) {
  console.log("=================================================");
  console.log("   Stopping Server...");
  console.log("=================================================");

  try {
    execSync("pm2 stop server && pm2 delete server", { stdio: "inherit" });
    process.exit(0);
  } catch (err) {
    console.error("   Error during stop:", err.message);
    process.exit(1);
  }
} else {
  const ip = getPrimaryIP();

  console.log("=================================================");
  console.log("   Starting Server...");

  if (ip) {
    console.log("   Server avilable at IP address:", ip);
  } else {
    console.log("   No IPv4 address detected.");
  }

  console.log("=================================================");

  try {
    execSync(`pm2 start index.js --name server -- ${ip}`, { stdio: "inherit" });
  } catch (err) {
    console.error("Error starting PM2:", err.message);
    process.exit(1);
  }
}
