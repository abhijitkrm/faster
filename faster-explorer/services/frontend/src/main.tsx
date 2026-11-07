import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import App from './App';
import { WSProvider } from './context/WSContext';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <BrowserRouter>
      <WSProvider>
        <App />
      </WSProvider>
    </BrowserRouter>
  </React.StrictMode>,
);
