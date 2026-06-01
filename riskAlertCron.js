// /backend/cron/riskAlertCron.js

import cron from "node-cron";

import {
  detectRoiSpike,
  detectMultiIpAbuse,
  detectWithdrawalAbuse,
  detectArbitragePattern,
} from "../services/riskService.js";

// ======================================
// PREVENT CRON OVERLAP
// ======================================

let isRiskCronRunning = false;

const startRiskAlertCron = () => {
  console.log("✅ Risk Alert Cron Started");

  cron.schedule(
    "* * * * *",
    async () => {
      // Prevent multiple cron executions at the same time
      if (isRiskCronRunning) {
        console.log(
          "⚠️ Previous Risk Cron is still running. Skipping this cycle.",
        );
        return;
      }

      isRiskCronRunning = true;

      try {
        console.log("⏰ Risk Cron Triggered");
        console.log("Running Risk Detection...");

        await detectRoiSpike();

        await detectMultiIpAbuse();

        await detectWithdrawalAbuse();

        await detectArbitragePattern();
      } catch (error) {
        console.log("RiskAlertCron Error:", error.message);
      } finally {
        // Always release lock
        isRiskCronRunning = false;
      }
    },
    {
      timezone: "UTC",
    },
  );
};

export default startRiskAlertCron;

/* 
import cron from "node-cron";

import {
  detectRoiSpike,
  detectMultiIpAbuse,
  detectWithdrawalAbuse,
  detectArbitragePattern,
} from "../services/riskService.js";

const startRiskAlertCron = () => {
  console.log("✅ Risk Alert Cron Started");

  cron.schedule(
    "* * * * *",
    async () => {
      try {
        console.log("⏰ Risk Cron Triggered");
        console.log("Running Risk Detection...");

        await detectRoiSpike();

        await detectMultiIpAbuse();

        await detectWithdrawalAbuse();

        await detectArbitragePattern();
      } catch (error) {
        console.log("RiskAlertCron Error:", error.message);
      }
    },
    {
      timezone: "UTC",
    },
  );
};

export default startRiskAlertCron;
 */