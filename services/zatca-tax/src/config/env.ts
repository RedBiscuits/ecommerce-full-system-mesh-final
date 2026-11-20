import dotenv from 'dotenv';

dotenv.config();

export const config = {
  port: parseInt(process.env.PORT || '3010', 10),
  nodeEnv: process.env.NODE_ENV || 'development',
  zatcaFatooraBaseUrl:
    process.env.ZATCA_FATOORA_BASE_URL ||
    'https://gw-fatoora.zatca.gov.sa/e-invoicing/developer-portal',
  zatcaReportingTimeoutMs: parseInt(process.env.ZATCA_REPORTING_TIMEOUT_MS || '5000', 10),
};
