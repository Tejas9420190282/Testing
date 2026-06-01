// /backend/services/riskService.js

import User from "../models/User.js";
import Trade from "../models/Trade.js";
import Transaction from "../models/Transaction.js";
import LoginHistory from "../models/LoginHistory.js";
import emailService from "./emailService.js";
import AuditLog from "../models/AuditLog.js";

import createRiskAlert from "./createRiskAlert.js";

// ======================================
// ROI SPIKE DETECTION
// ======================================

export const detectRoiSpike = async () => {
  try {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);

    console.log("One Hour Ago:", oneHourAgo);
    /* 
    const trades = await Trade.find({
      status: "CLOSED",
      closedAt: {
        $gte: oneHourAgo,
      },
      realizedPnl: { $gt: 0 },
    });
 */
    const trades = await Trade.aggregate([
      {
        $match: {
          status: "CLOSED",
          closedAt: {
            $gte: oneHourAgo,
          },
          realizedPnl: {
            $gt: 0,
          },
        },
      },
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user",
        },
      },
      {
        $unwind: "$user",
      },
    ]);

    console.log("Trades Length:", trades.length);
    console.log("Trades:", trades);

    for (const trade of trades) {
      /* 
        const user = await User.findById(trade.userId);

      if (!user) continue;
 */
      const user = trade.user;

      if (!user) continue;

      // Avoid division by zero
      if (user.walletBalance <= 0) continue;

      const roi = (trade.realizedPnl / user.walletBalance) * 100;

      console.log("User Wallet:", user.walletBalance);
      console.log("Trade PNL:", trade.realizedPnl);
      console.log("Calculated ROI:", roi);
      console.log("ROI > 500 ?", roi > 500);

      if (roi > 500) {
        /* 
        // Update user risk
        user.riskLevel = "CRITICAL";

        user.lastRiskDetectedAt = new Date();

        if (!user.riskFlags.includes("ROI_SPIKE")) {
          user.riskFlags.push("ROI_SPIKE");

          user.riskScore += 100;
        }

        if (user.isModified()) {
          await user.save();
        }
 */

        const currentUser = await User.findById(user._id).select(
          "riskFlags riskScore",
        );

        const updateData = {
          riskLevel: "CRITICAL",
          lastRiskDetectedAt: new Date(),
        };

        if (!currentUser.riskFlags.includes("ROI_SPIKE")) {
          updateData.riskFlags = [...currentUser.riskFlags, "ROI_SPIKE"];

          updateData.riskScore = currentUser.riskScore + 100;
        }

        await User.updateOne(
          { _id: user._id },
          {
            $set: updateData,
          },
        );

        // Create alert
        const riskAlert = await createRiskAlert({
          userId: user._id,
          type: "ROI_SPIKE",
          severity: "CRITICAL",
          message: `ROI spike detected (${roi.toFixed(2)}%)`,
          metadata: {
            tradeId: trade._id,
            roi,
            pnl: trade.realizedPnl,
          },
        });

        if (riskAlert.isNew) {
          try {
            await emailService.sendAdminNotificationEmail({
              subject: "🚨 ROI Spike Detected",

              heading: "ROI Spike Detected",

              message:
                "A suspicious ROI spike has been detected in the trading system.",

              rows: [
                ["User Email", user.email],
                ["User ID", user.uniqueUserId],
                ["ROI", `${roi.toFixed(2)}%`],
                ["PNL", trade.realizedPnl],
                ["Trade ID", trade._id],
                ["Risk Level", user.riskLevel],
              ],

              metadata: {
                type: "ROI_SPIKE",
                userId: user._id,
                tradeId: trade._id,
              },
            });
          } catch (error) {
            console.log("Risk Alert Email Error:", error.message);
          }
        }

        // Audit log
        const existingAudit = await AuditLog.findOne({
          userId: user._id,

          action: "ROI_SPIKE_DETECTED",

          createdAt: {
            $gte: oneHourAgo,
          },
        });

        if (!existingAudit) {
          await AuditLog.create({
            userId: user._id,

            action: "ROI_SPIKE_DETECTED",

            module: "RISK_ENGINE",

            severity: "CRITICAL",

            targetType: "Trade",

            targetId: trade._id,

            details: {
              roi,
              pnl: trade.realizedPnl,
            },
          });
        }
      }
    }
  } catch (error) {
    console.log("detectRoiSpike Error:", error.message);
  }
};

// ======================================
// MULTI IP ABUSE
// ======================================
/* 
export const detectMultiIpAbuse = async () => {
  try {
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const users = await User.find();

    for (const user of users) {
      const logs = await LoginHistory.find({
        userId: user._id,
        createdAt: {
          $gte: oneDayAgo,
        },
      });

      const countries = [
        ...new Set(logs.map((log) => log.country).filter(Boolean)),
      ];

      console.log("User:", user.email);
      console.log("Countries:", countries);
      console.log("Countries Length:", countries.length);

      if (countries.length >= 5) {
        user.riskLevel = "HIGH";

        user.lastRiskDetectedAt = new Date();

        if (!user.riskFlags.includes("MULTI_IP_ABUSE")) {
          user.riskFlags.push("MULTI_IP_ABUSE");

          user.riskScore += 50;
        }

        if (user.isModified()) {
          await user.save();
        }

        const riskAlert = await createRiskAlert({
          userId: user._id,
          type: "MULTI_IP_ABUSE",
          severity: "HIGH",
          message: "Multiple countries detected",
          metadata: {
            countries,
          },
        });

        if (riskAlert.isNew) {
          try {
               console.log("🚨 Sending Multi-IP Alert Email...");

            await emailService.sendAdminNotificationEmail({
              subject: "🚨 Multi-IP Abuse Detected",

              heading: "Multiple Country Login Detected",

              message:
                "A user logged in from multiple countries within 24 hours.",

              rows: [
                ["User Email", user.email],
                ["User ID", user.uniqueUserId],
                ["Countries", countries.join(", ")],
                ["Country Count", countries.length],
                ["Risk Level", user.riskLevel],
              ],

              metadata: {
                type: "MULTI_IP_ABUSE",
                userId: user._id,
              },
            });

            console.log("✅ Multi-IP Alert Email Sent");
          } catch (error) {
            console.log("Risk Alert Email Error:", error.message);
          }
        }

        const existingAudit = await AuditLog.findOne({
          userId: user._id,

          action: "MULTI_IP_ABUSE",

          createdAt: {
            $gte: oneDayAgo,
          },
        });

        if (!existingAudit) {
          await AuditLog.create({
            userId: user._id,

            action: "MULTI_IP_ABUSE",

            module: "RISK_ENGINE",

            severity: "WARNING",

            targetType: "LoginHistory",

            targetId: logs[0]?._id,

            details: {
              countries,
            },
          });
        }
      }
    }
  } catch (error) {
    console.log("detectMultiIpAbuse Error:", error.message);
  }
};
 */
export const detectMultiIpAbuse = async () => {
  try {
    const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const suspiciousUsers = await LoginHistory.aggregate([
      {
        $match: {
          createdAt: {
            $gte: oneDayAgo,
          },
          country: {
            $nin: ["", null],
          },
        },
      },
      {
        $group: {
          _id: "$userId",
          countries: {
            $addToSet: "$country",
          },
          loginId: {
            $first: "$_id",
          },
        },
      },
      {
        $addFields: {
          countryCount: {
            $size: "$countries",
          },
        },
      },
      {
        $match: {
          countryCount: {
            $gte: 5,
          },
        },
      },
    ]);

    for (const item of suspiciousUsers) {
      const user = await User.findById(item._id);

      if (!user) continue;

      const countries = item.countries;

      user.riskLevel = "HIGH";
      user.lastRiskDetectedAt = new Date();

      if (!user.riskFlags.includes("MULTI_IP_ABUSE")) {
        user.riskFlags.push("MULTI_IP_ABUSE");
        user.riskScore += 50;
      }

      if (user.isModified()) {
        await user.save();
      }

      const riskAlert = await createRiskAlert({
        userId: user._id,
        type: "MULTI_IP_ABUSE",
        severity: "HIGH",
        message: "Multiple countries detected",
        metadata: {
          countries,
        },
      });

      if (riskAlert.isNew) {
        try {
          console.log("🚨 Sending Multi-IP Alert Email...");

          await emailService.sendAdminNotificationEmail({
            subject: "🚨 Multi-IP Abuse Detected",

            heading: "Multiple Country Login Detected",

            message:
              "A user logged in from multiple countries within 24 hours.",

            rows: [
              ["User Email", user.email],
              ["User ID", user.uniqueUserId],
              ["Countries", countries.join(", ")],
              ["Country Count", countries.length],
              ["Risk Level", user.riskLevel],
            ],

            metadata: {
              type: "MULTI_IP_ABUSE",
              userId: user._id,
            },
          });

          console.log("✅ Multi-IP Alert Email Sent");
        } catch (error) {
          console.log("Risk Alert Email Error:", error.message);
        }
      }

      const existingAudit = await AuditLog.findOne({
        userId: user._id,
        action: "MULTI_IP_ABUSE",
        createdAt: {
          $gte: oneDayAgo,
        },
      });

      if (!existingAudit) {
        await AuditLog.create({
          userId: user._id,

          action: "MULTI_IP_ABUSE",

          module: "RISK_ENGINE",

          severity: "WARNING",

          targetType: "LoginHistory",

          targetId: item.loginId,

          details: {
            countries,
          },
        });
      }
    }
  } catch (error) {
    console.log("detectMultiIpAbuse Error:", error.message);
  }
};

// ======================================
// WITHDRAWAL ABUSE
// ======================================
/* 
export const detectWithdrawalAbuse = async () => {
  try {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);

    const users = await User.find();

    for (const user of users) {
      const failedWithdrawals = await Transaction.find({
        userId: user._id,

        type: "Withdrawal",

        status: "Rejected",

        createdAt: {
          $gte: oneHourAgo,
        },
      });

      if (failedWithdrawals.length >= 3) {
        // Prevent repeated freeze
        if (!user.withdrawalFrozen) {
          user.withdrawalFrozen = true;

          user.riskLevel = "HIGH";

          user.lastRiskDetectedAt = new Date();

          if (!user.riskFlags.includes("WITHDRAWAL_ABUSE")) {
            user.riskFlags.push("WITHDRAWAL_ABUSE");

            user.riskScore += 50;
          }

          if (user.isModified()) {
            await user.save();
          }
        }

        const riskAlert = await createRiskAlert({
          userId: user._id,
          type: "WITHDRAWAL_ABUSE",
          severity: "HIGH",
          message: "Multiple failed withdrawals detected",
          metadata: {
            failedAttempts: failedWithdrawals.length,
          },
        });

        if (riskAlert.isNew) {
          try {
            await emailService.sendAdminNotificationEmail({
              subject: "🚨 Withdrawal Abuse Detected",

              heading: "Withdrawal Abuse Alert",

              message: "Multiple failed withdrawal attempts detected.",

              rows: [
                ["User Email", user.email],
                ["User ID", user.uniqueUserId],
                ["Failed Attempts", failedWithdrawals.length],
                ["Withdrawal Frozen", user.withdrawalFrozen],
                ["Risk Level", user.riskLevel],
              ],

              metadata: {
                type: "WITHDRAWAL_ABUSE",
                userId: user._id,
              },
            });
          } catch (error) {
            console.log("Risk Alert Email Error:", error.message);
          }
        }

        const existingAudit = await AuditLog.findOne({
          userId: user._id,

          action: "WITHDRAWAL_FROZEN",

          createdAt: {
            $gte: oneHourAgo,
          },
        });

        if (!existingAudit) {
          await AuditLog.create({
            userId: user._id,

            action: "WITHDRAWAL_FROZEN",

            module: "RISK_ENGINE",

            severity: "CRITICAL",

            targetType: "User",

            targetId: user._id,

            details: {
              failedAttempts: failedWithdrawals.length,
            },
          });
        }
      }
    }
  } catch (error) {
    console.log("detectWithdrawalAbuse Error:", error.message);
  }
};
 */
export const detectWithdrawalAbuse = async () => {
  try {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);

    const suspiciousUsers = await Transaction.aggregate([
      {
        $match: {
          type: "Withdrawal",
          status: "Rejected",
          createdAt: {
            $gte: oneHourAgo,
          },
        },
      },
      {
        $group: {
          _id: "$userId",
          failedAttempts: {
            $sum: 1,
          },
        },
      },
      {
        $match: {
          failedAttempts: {
            $gte: 3,
          },
        },
      },
    ]);

    for (const item of suspiciousUsers) {
      const user = await User.findById(item._id);

      if (!user) continue;

      if (!user.withdrawalFrozen) {
        user.withdrawalFrozen = true;

        user.riskLevel = "HIGH";

        user.lastRiskDetectedAt = new Date();

        if (!user.riskFlags.includes("WITHDRAWAL_ABUSE")) {
          user.riskFlags.push("WITHDRAWAL_ABUSE");
          user.riskScore += 50;
        }

        if (user.isModified()) {
          await user.save();
        }
      }

      const riskAlert = await createRiskAlert({
        userId: user._id,
        type: "WITHDRAWAL_ABUSE",
        severity: "HIGH",
        message: "Multiple failed withdrawals detected",
        metadata: {
          failedAttempts: item.failedAttempts,
        },
      });

      if (riskAlert.isNew) {
        try {
          await emailService.sendAdminNotificationEmail({
            subject: "🚨 Withdrawal Abuse Detected",

            heading: "Withdrawal Abuse Alert",

            message: "Multiple failed withdrawal attempts detected.",

            rows: [
              ["User Email", user.email],
              ["User ID", user.uniqueUserId],
              ["Failed Attempts", item.failedAttempts],
              ["Withdrawal Frozen", user.withdrawalFrozen],
              ["Risk Level", user.riskLevel],
            ],

            metadata: {
              type: "WITHDRAWAL_ABUSE",
              userId: user._id,
            },
          });
        } catch (error) {
          console.log("Risk Alert Email Error:", error.message);
        }
      }

      const existingAudit = await AuditLog.findOne({
        userId: user._id,
        action: "WITHDRAWAL_FROZEN",
        createdAt: {
          $gte: oneHourAgo,
        },
      });

      if (!existingAudit) {
        await AuditLog.create({
          userId: user._id,

          action: "WITHDRAWAL_FROZEN",

          module: "RISK_ENGINE",

          severity: "CRITICAL",

          targetType: "User",

          targetId: user._id,

          details: {
            failedAttempts: item.failedAttempts,
          },
        });
      }
    }
  } catch (error) {
    console.log("detectWithdrawalAbuse Error:", error.message);
  }
};

// ======================================
// ARBITRAGE DETECTION
// ======================================

export const detectArbitragePattern = async () => {
  try {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    /* 
    const trades = await Trade.find({
      status: "CLOSED",
      closedAt: {
        $gte: oneHourAgo,
      },
    });
 */

    const trades = await Trade.aggregate([
      {
        $match: {
          status: "CLOSED",
          closedAt: {
            $gte: oneHourAgo,
          },
        },
      },
      {
        $lookup: {
          from: "users",
          localField: "userId",
          foreignField: "_id",
          as: "user",
        },
      },
      {
        $unwind: "$user",
      },
    ]);

    for (const trade of trades) {
      if (!trade.closedAt || !trade.openedAt) continue;

      const duration = trade.closedAt - trade.openedAt;

      // Less than 1 second
      if (duration < 1000) {
        /*  const user = await User.findById(trade.userId);

        if (!user) continue;
 */
        const user = trade.user;

        if (!user) continue;
        /*         
        user.riskLevel = "HIGH";

        user.lastRiskDetectedAt = new Date();

        if (!user.riskFlags.includes("ARBITRAGE_PATTERN")) {
          user.riskFlags.push("ARBITRAGE_PATTERN");

          user.riskScore += 40;
        }
        if (user.isModified()) {
          await user.save();
        }
 */
        const currentUser = await User.findById(user._id).select(
          "riskFlags riskScore",
        );

        const updateData = {
          riskLevel: "HIGH",
          lastRiskDetectedAt: new Date(),
        };

        if (!currentUser.riskFlags.includes("ARBITRAGE_PATTERN")) {
          updateData.riskFlags = [
            ...currentUser.riskFlags,
            "ARBITRAGE_PATTERN",
          ];

          updateData.riskScore = currentUser.riskScore + 40;
        }

        await User.updateOne(
          { _id: user._id },
          {
            $set: updateData,
          },
        );

        const riskAlert = await createRiskAlert({
          userId: user._id,
          type: "ARBITRAGE_PATTERN",
          severity: "HIGH",
          message: "Millisecond trading pattern detected",
          metadata: {
            tradeId: trade._id,
            duration,
            symbol: trade.symbol,
          },
        });

        if (riskAlert.isNew) {
          try {
            await emailService.sendAdminNotificationEmail({
              subject: "🚨 Arbitrage Pattern Detected",

              heading: "Millisecond Trading Detected",

              message: "Potential arbitrage or bot trading activity detected.",

              rows: [
                ["User Email", user.email],
                ["User ID", user.uniqueUserId],
                ["Trade Symbol", trade.symbol],
                ["Duration", `${duration} ms`],
                ["Trade ID", trade._id],
                ["Risk Level", user.riskLevel],
              ],

              metadata: {
                type: "ARBITRAGE_PATTERN",
                userId: user._id,
                tradeId: trade._id,
              },
            });
          } catch (error) {
            console.log("Risk Alert Email Error:", error.message);
          }
        }

        const existingAudit = await AuditLog.findOne({
          userId: user._id,
          action: "ARBITRAGE_PATTERN_DETECTED",
          createdAt: {
            $gte: oneHourAgo,
          },
        });

        if (!existingAudit) {
          await AuditLog.create({
            userId: user._id,

            action: "ARBITRAGE_PATTERN_DETECTED",

            module: "RISK_ENGINE",

            severity: "WARNING",

            targetType: "Trade",

            targetId: trade._id,

            details: {
              duration,
              symbol: trade.symbol,
            },
          });
        }
      }
    }
  } catch (error) {
    console.log("detectArbitragePattern Error:", error.message);
  }
};
