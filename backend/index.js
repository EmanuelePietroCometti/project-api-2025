import express from 'express';
import morgan from 'morgan';
import cors from 'cors';
import listRoutes from './routes/listRoutes.js';
import filesRoutes from './routes/filesRoutes.js';
import mkdirRoutes from './routes/mkdirRoutes.js';

// init express
const app = new express();
app.use(morgan('dev'));
app.use(express.json());

const port = 3001;

const corsOptions = {
  origin: 'http://localhost:5173/',
  credentials: true
};
app.use(cors(corsOptions));

// API
app.use('/list', listRoutes);
app.use('/files', filesRoutes);
app.use('/mkdir', mkdirRoutes);


// activate the server
app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});
