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

const ip = getPrimaryIP();

console.log("=================================================");
console.log("   Starting backend API...");

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