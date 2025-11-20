import express from 'express';
import morgan from 'morgan';
import cors from 'cors';
import listRoutes from './routes/listRoutes.js';
import filesRoutes from './routes/filesRoutes.js';
import mkdirRoutes from './routes/mkdirRoutes.js';
import statsRoutes from './routes/statsRoutes.js';
import os from 'os';

// init express
const app = new express();
app.use(morgan('dev'));
app.use(express.json());

const port = 3001;
const interfaces = os.networkInterfaces(); 
const addresses = [];

for (let iface in interfaces) {
  for (let addr of interfaces[iface]) {
    if (addr.family === "IPv4" && !addr.internal) {
      addresses.push(addr.address);
    }
  }
}
const originAddress = `http://${addresses[0]}:5173/`;

const corsOptions = {
  origin: originAddress,
  credentials: true
};
app.use(cors(corsOptions));

// API
app.use('/list', listRoutes);
app.use('/files', filesRoutes);
app.use('/mkdir', mkdirRoutes);
app.use('/stats', statsRoutes);


// activate the server
app.listen(port, () => {
  console.log(`Server listening at ${originAddress}`);
});
