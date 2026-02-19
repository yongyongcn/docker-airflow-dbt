SELECT UnionInvoice.Coef                                                               AS `Coef`,
       UnionInvoice.IdInvoice                                                          AS `IdInvoice`,
       UnionInvoice.IdProject                                                          AS `IdProject`,
       UnionInvoice.TransferDateFormat                                                 AS `TransferDateFormat`,
       UnionInvoice.V1                                                                 AS `V1`,
       UnionInvoice.V2                                                                 AS `V2`,
       UnionInvoice.V3                                                                 AS `V3`,
       UnionInvoice.OutProject                                                         AS `OutProject`,
       UnionInvoice.IdInstance                                                         AS `IdInstance`,
       Instance.code                                                                   AS `CodeInstance`,
       IFNULL(InvoiceFinance.paymentAmount, Invoice.sum) * UnionInvoice.Coef           AS `CalcPaymentAmount`,
       UnionInvoice.PaymentSum                                                         AS `PaymentSum`,
       CurrencyPayment.show_name                                                       AS `IsoCurrencyPayment`,
       CurrencyDraft.iso_code                                                          AS `IsoCurrencyPayoutReport`,
       CurrencyDraft.iso_code                                                          AS `IsoCurrencyDraft`,
       IF(Invoice.status = 4 OR Invoice.status = 16, BillingParameterValues.value, '') AS `RefundReason`,
       UnionInvoice.IdStatus                                                           AS `IdStatus`,
       InvoiceStatus.name                                                              AS `NameStatus`,
       UnionInvoice.StatusXsolla                                                       AS `StatusXsolla`,
       UnionInvoice.TestProject                                                        AS `TestProject`,
       InvoiceInfo.foreignInvoice                                                      AS `ForeignInvoice`,
       CurrencyProject.iso_code                                                        AS `IsoCurrencyProject`,
       ROUND(InvoiceInfo.project_amount, 4)                                            AS `AmountProject`,
       InvoiceFinance.salesTaxCommissionPrc                                            AS `SalesTaxCommissionPrc`,
       salesTaxCommission * UnionInvoice.Coef                                          AS `SalesTaxCommission`,
       InvoiceFinance.paymentAmount                                                    AS `PaymentAmount`,
       InvoiceFinance.sumNominalFromPsPayment                                          AS `SumNominalFromPsPayment`,
       InvoiceFinance.sumPayoutFromPsPayment                                           AS `SumPayoutFromPsPayment`,
       InvoiceFinance.paymentUserWalletAmount                                          AS `PaymentUserWalletAmount`,
       InvoiceFinance.paymentUserWalletNominalAmount                                   AS `PaymentUserWalletNominalAmount`,
       InvoiceFinance.paymentUserWalletPayoutAmount                                    AS `PaymentUserWalletPayoutAmount`,
       InvoiceFinance.userWalletAmount                                                 AS `UserWalletAmount`,
       InvoiceFinance.userWalletNominalAmount                                          AS `UserWalletNominalAmount`,
       InvoiceFinance.userWalletPayoutAmount                                           AS `UserWalletPayoutAmount`,
       UserWalletCurrency.iso_code                                                     AS `UserWalletCurrencyIso`,
       InvoiceOrder.idOrder                                                            AS `StoreOrderId`,
       IF(
               InvoiceOrder.idOrder IS NULL,
               NULL,
               (SELECT GROUP_CONCAT(
                               CONCAT_WS(', ', bio.sku, bio.quantity) SEPARATOR '; '
                       )
                FROM billing.invoice_order_line bio
                WHERE bio.order_id = InvoiceOrder.idOrder)
       )                                                                               AS `StoreCart`,
       InvoiceShippingDetails.address                                                  AS `ShippingAddress`,
       Project.localizedName                                                           AS `LocalizedNameProject`,
       UnionInvoice.CreateDate                                                         AS `CreateDate`,
       Project.href_en                                                                 AS `NameProject`,
       Project.name_en                                                                 AS `NameVirtualCurrencyProject`,
       UnionInvoice.CommissionXsolla                                                   AS `CommissionXsolla`,
       UnionInvoice.CommissionXsollaPrc                                                AS `CommissionXsollaPrc`,
       UnionInvoice.CommissionPs                                                       AS `CommissionPs`,
       UnionInvoice.CommissionPsPrc                                                    AS `CommissionPsPrc`,
       UnionInvoice.Fee                                                                AS `Fee`,
       InvoiceFinance.commission_xsolla_fee                                            AS `CommissionXsollaFee`,
       UnionInvoice.CommissionAgentFee                                                 AS `CommissionAgentFee`,
       ROUND(
               ROUND(
                       UnionInvoice.Coef * (
                           Invoice.sum_payout + IF(
                                   UnionInvoice.Coef = -1,
                                   (
                                       CanceledInvoice.chargebackPenalty + CanceledInvoice.refundPenalty
                                       ),
                                   0
                                                )
                           ),
                       CurrencyPayment.precision
               ),
               4
       )                                                                               AS `PayoutSum`,
       UnionInvoice.PayoutPrc                                                          AS `PayoutPrc`,
       ROUND(
               (
                   IFNULL(
                           InvoiceFinance.paymentAmount, Invoice.sum
                   ) - ROUND(Invoice.sum_nominal, 2)
                   ) * UnionInvoice.Coef,
               4
       )                                                                               AS `UserCommission`,
       ROUND(
               IFNULL(
                       (
                           IFNULL(
                                   InvoiceFinance.paymentAmount, Invoice.sum
                           ) - ROUND(Invoice.sum_nominal, 2)
                           ) * 100 / IFNULL(
                               InvoiceFinance.paymentAmount, Invoice.sum
                                     ),
                       0
               ),
               2
       )                                                                               AS `UserCommissionPrc`,
       UnionInvoice.CommissionAgentReal                                                AS `CommissionAgentReal`,
       UnionInvoice.CommissionAgentPrc                                                 AS `CommissionAgentPrc`,
       UnionInvoice.NominalSum                                                         AS `NominalSum`,
       Country.ISO                                                                     AS `IsoCountry`,
       UnionInvoice.Phone                                                              AS `Phone`,
       UnionInvoice.FilteredEmail                                                      AS `FilteredEmail`,
       IFNULL(
               PaysystemsExternalFees.commission_percent,
               PaysystemsExternalFeesAnyProject.commission_percent
       )                                                                               AS `CommissionPercentPaysystemsExternalFees`,
       IFNULL(
               PaysystemsExternalFees.commission_amount,
               PaysystemsExternalFeesAnyProject.commission_amount
       )                                                                               AS `CommissionAmountPaysystemsExternalFees`,
       IFNULL(
               PaysystemsExternalFees.commission_currency,
               PaysystemsExternalFeesAnyProject.commission_currency
       )                                                                               AS `CommissionCurrencyPaysystemsExternalFees`,
       IF(
               0,
               PsHoldInvoice.rate,
               IF(
                       UnionInvoice.Coef < 0 && CanceledInvoice.rate IS NOT NULL && (
                           Invoice.currency_draft != CanceledInvoice.currency_draft
                           ),
                       CanceledInvoice.rate,
                       COALESCE(
                               Invoice.rate_draft,
                               IF(
                                       Agreement.multicurrency, NULL, Invoice.rate_transfer
                               ),
                               ROUND(
                                       IFNULL(
                                               CurrencyPaymentHistory.kurs / CurrencyDraftHistory.kurs,
                                               CurrencyPayment.kurs / CurrencyDraft.kurs
                                       ) / (
                                           100 + IFNULL(
                                                   InvoiceInfo.commission_rate_prc,
                                                   0
                                                 )
                                           ) * 100 / (
                                           100 + IFNULL(
                                                   InvoiceInfo.commission_rate_project_prc,
                                                   0
                                                 )
                                           ) * 100,
                                       10
                               )
                       )
               )
       )                                                                               AS `CalcRate`,
       CommissionXsollaVatShow * UnionInvoice.Coef                                     AS `CommissionXsollaVat`,
       InvoiceFinance.commissionXsollaVatPrc                                           AS `CommissionXsollaVatPrc`,
       InvoiceInfo.sum_project_nominal                                                 AS `AmountProjectNominal`,
       CurrencyProjectNominal.iso_code                                                 AS `IsoCurrencyProjectNominal`,
       InvoiceFinance.repatriationCommission * UnionInvoice.Coef                       AS `RepatriationCommission`,
       InvoiceFinance.repatriationCommissionPrc                                        AS `RepatriationCommissionPrc`,
       Refer.name                                                                      AS `NameRefer`,
       CurrencyPayout.show_name                                                        AS `IsoCurrencyPayout`,
       InvoiceFinance.commissionUser                                                   AS `CommissionUser`,
       InvoiceInfo.foreignRequest                                                      AS `ForeignRequest`,
       PsInstanceClass.ClassCodes                                                      AS `ClassesInstance`,
       commission_user_vat * UnionInvoice.Coef                                         AS `CommissionUserVat`,
       InvoiceFinance.commission_user_vat_prc                                          AS `CommissionUserVatPrc`,
       BillingRefundCases.comment                                                      AS `CommentRefundReason`,
       InvoiceFinance.isPsPayout                                                       AS `IsPsPayout`,
       cpaNetworkCommission * UnionInvoice.Coef                                        AS `CpaNetworkCommission`,
       InvoiceFinance.cpaNetworkCommissionPrc                                          AS `CpaNetworkCommissionPrc`,
       UnionInvoice.ChargebackPenalty                                                  AS `ChargebackPenalty`,
       UnionInvoice.RefundPenalty                                                      AS `RefundPenalty`,
       InvoiceGiftForFriendDetails.recipientEmail                                      AS `EmailFriend`,
       InvoiceGiftForFriendDetails.recipientId                                         AS `IdFriend`,
       InvoiceParent.parent_invoice_id                                                 AS `IdParentInvoice`,
       UnionInvoice.UserIp                                                             AS `UserIp`,
       InvoiceDirectTaxes.directTaxesPercent                                           AS `DirectWhtTotalPercent`,
       directTaxesAmount * UnionInvoice.Coef                                           AS `DirectWhtTotalAmount`,
       UnionInvoice.TransferDate                                                       AS `TransferDate`
FROM ((SELECT STRAIGHT_JOIN 1                                      AS `Coef`,
                            Invoice.id                             AS `IdInvoice`,
                            Invoice.id_project                     AS `IdProject`,
                            DATE_FORMAT(
                                    IFNULL(
                                            Invoice.date_payout,
                                            IFNULL(
                                                    Invoice.date_payment, Invoice.date_create
                                            )
                                    ),
                                    '%d.%m.%Y %H:%i'
                            )                                      AS `TransferDateFormat`,
                            Invoice.v1                             AS `V1`,
                            Invoice.v2                             AS `V2`,
                            Invoice.v3                             AS `V3`,
                            ROUND(Invoice.out, 4)                  AS `OutProject`,
                            Invoice.id_instance                    AS `IdInstance`,
                            ROUND(Invoice.sum, 4)                  AS `PaymentSum`,
                            Invoice.status                         AS `IdStatus`,
                            Invoice.status_xsolla                  AS `StatusXsolla`,
                            Invoice.test_project                   AS `TestProject`,
                            Invoice.date_create                    AS `CreateDate`,
                            ROUND(Invoice.commission_xsolla, 4)    AS `CommissionXsolla`,
                            ROUND(
                                    Invoice.commission_xsolla_show_prc,
                                    2
                            )                                      AS `CommissionXsollaPrc`,
                            ROUND(Invoice.commission_ps, 4)        AS `CommissionPs`,
                            ROUND(
                                    Invoice.commission_ps_show_prc,
                                    2
                            )                                      AS `CommissionPsPrc`,
                            ROUND(Invoice.commission_ps_fee, 4)    AS `Fee`,
                            ROUND(Invoice.commission_agent_fee, 4) AS `CommissionAgentFee`,
                            ROUND(
                                    100 - ROUND(Invoice.commission_agent_prc, 2),
                                    2
                            )                                      AS `PayoutPrc`,
                            ROUND(Invoice.commission_agent, 4)     AS `CommissionAgentReal`,
                            ROUND(Invoice.commission_agent_prc, 2) AS `CommissionAgentPrc`,
                            ROUND(Invoice.sum_nominal, 2) * 1      AS `NominalSum__mod3`,
                            ROUND(
                                    ROUND(Invoice.sum_nominal, 2) * 1,
                                    4
                            )                                      AS `NominalSum`,
                            Invoice.phone                          AS `Phone`,
                            IF(
                                    Invoice.email = ''
                                        OR Invoice.email = 'support@xsolla.com'
                                        OR Invoice.email REGEXP '^[a-f0-9]{32}@xsolla[.]com$',
                                    NULL,
                                    Invoice.email
                            )                                      AS `FilteredEmail`,
                            0                                      AS `ChargebackPenalty`,
                            0                                      AS `RefundPenalty`,
                            IFNULL(
                                    INET_NTOA(Invoice.user_ip),
                                    ''
                            )                                      AS `UserIp`,
                            Invoice.date_payout                    AS `TransferDate`
       FROM billing.invoice Invoice
                LEFT JOIN dvapay.bank_back CanceledInvoice ON CanceledInvoice.id_transfer = Invoice.id_demand
       WHERE Invoice.test_project IN (0)
         AND Invoice.id_bank_contr IN (207429)
         AND Invoice.id_project IN (137183, 148492, 183614)
         AND Invoice.date_payout >= '2022-09-22 00:00:00'
         AND Invoice.date_payout < '2022-09-30 00:00:00'
         AND Invoice.status IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
       GROUP BY Coef, TransferDate, IdInvoice
       ORDER BY TransferDate DESC, IdInvoice DESC
       LIMIT 500)
      UNION ALL
      (SELECT -1                                                     AS `Coef`,
              Invoice.id                                             AS `IdInvoice`,
              Invoice.id_project                                     AS `IdProject`,
              DATE_FORMAT(CanceledInvoice.data, '%d.%m.%Y %H:%i')    AS `TransferDateFormat`,
              Invoice.v1                                             AS `V1`,
              Invoice.v2                                             AS `V2`,
              Invoice.v3                                             AS `V3`,
              ROUND(Invoice.out * -1, 4)                             AS `OutProject`,
              Invoice.id_instance                                    AS `IdInstance`,
              ROUND(Invoice.sum * -1, 4)                             AS `PaymentSum`,
              Invoice.status                                         AS `IdStatus`,
              Invoice.status_xsolla                                  AS `StatusXsolla`,
              Invoice.test_project                                   AS `TestProject`,
              Invoice.date_create                                    AS `CreateDate`,
              ROUND(Invoice.commission_xsolla * -1, 4)               AS `CommissionXsolla`,
              ROUND(Invoice.commission_xsolla_show_prc, 2)           AS `CommissionXsollaPrc`,
              ROUND(Invoice.commission_ps * -1, 4)                   AS `CommissionPs`,
              ROUND(Invoice.commission_ps_show_prc, 2)               AS `CommissionPsPrc`,
              Invoice.commission_ps_fee                              AS `Fee__mod8`,
              ROUND(Invoice.commission_ps_fee, 4)                    AS `Fee`,
              ROUND(Invoice.commission_agent_fee, 4)                 AS `CommissionAgentFee`,
              ROUND(100 - ROUND(Invoice.commission_agent_prc, 2), 2) AS `PayoutPrc`,
              ROUND(-1 * (Invoice.commission_agent -
                          (CanceledInvoice.chargebackPenalty + CanceledInvoice.refundPenalty)),
                    4)                                               AS `CommissionAgentReal`,
              ROUND(Invoice.commission_agent_prc, 2)                 AS `CommissionAgentPrc`,
              ROUND(ROUND(Invoice.sum_nominal, 2) * -1, 4)           AS `NominalSum`,
              Invoice.phone                                          AS `Phone`,
              IF(Invoice.email = '' OR Invoice.email = 'support@xsolla.com' OR
                 Invoice.email REGEXP '^[a-f0-9]{32}@xsolla[.]com$', NULL,
                 Invoice.email)                                      AS `FilteredEmail`,
              -1.0 * CanceledInvoice.chargebackPenalty               AS `ChargebackPenalty`,
              -1.0 * CanceledInvoice.refundPenalty                   AS `RefundPenalty`,
              IFNULL(INET_NTOA(Invoice.user_ip), '')                 AS `UserIp`,
              CanceledInvoice.data                                   AS `TransferDate`
       FROM billing.invoice Invoice
                LEFT JOIN dvapay.bank_back CanceledInvoice ON CanceledInvoice.id_transfer = Invoice.id_demand
       WHERE Invoice.test_project IN (0)
         AND Invoice.id_bank_contr IN (207429)
         AND Invoice.id_project IN (137183, 148492, 183614)
         AND CanceledInvoice.data >= '2022-09-22 00:00:00'
         AND CanceledInvoice.data < '2022-09-30 00:00:00'
         AND Invoice.status IN (1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
       GROUP BY Coef, TransferDate, IdInvoice
       ORDER BY TransferDate DESC, IdInvoice DESC
       LIMIT 500)
      ORDER BY TransferDate DESC, IdInvoice DESC
      LIMIT 500) UnionInvoice
         INNER JOIN billing.invoice Invoice ON Invoice.id = UnionInvoice.IdInvoice
         LEFT JOIN dvapay.paysystems_instance Instance ON Instance.id = Invoice.id_instance
         LEFT JOIN billing.invoice_finance InvoiceFinance ON InvoiceFinance.id_invoice = Invoice.id
         LEFT JOIN dvapay.valuta CurrencyPayment ON CurrencyPayment.id = Invoice.currency_payment
         LEFT JOIN dvapay.bank_back CanceledInvoice ON CanceledInvoice.id_transfer = Invoice.id_demand
         LEFT JOIN dvapay.bank_draft Draft
                   ON Draft.id = IF(UnionInvoice.Coef = -1, CanceledInvoice.id_draft, Invoice.id_draft)
         LEFT JOIN dvapay.bank_agreement Agreement ON Agreement.id = IF(UnionInvoice.Coef = -1 AND
                                                                        CanceledInvoice.id_agreement IS NOT NULL,
                                                                        CanceledInvoice.id_agreement,
                                                                        Invoice.id_agreement)
         LEFT JOIN dvapay.valuta CurrencyDraft
                   ON CurrencyDraft.id = IFNULL(Draft.currency_id, IF(UnionInvoice.Coef = -1 AND
                                                                      CanceledInvoice.rate IS NOT NULL AND
                                                                      CanceledInvoice.currency_draft IS NOT NULL AND
                                                                      CanceledInvoice.currency_draft !=
                                                                      Invoice.currency_draft,
                                                                      CanceledInvoice.currency_draft,
                                                                      IFNULL(Invoice.currency_draft, Agreement.id_currency_draft)))
         LEFT JOIN billing.refund_cases BillingRefundCases ON BillingRefundCases.id_invoice = Invoice.id
         LEFT JOIN billing.invoice_statuses InvoiceStatus ON InvoiceStatus.id = Invoice.status
         LEFT JOIN billing.invoice_info InvoiceInfo ON InvoiceInfo.id_invoice = Invoice.id
         LEFT JOIN billing.invoice_order InvoiceOrder ON InvoiceOrder.idInvoice = Invoice.id
         LEFT JOIN billing.invoice_shipping_details InvoiceShippingDetails
                   ON InvoiceShippingDetails.idInvoice = Invoice.id
         LEFT JOIN dvapay.money Project ON Project.id = Invoice.id_project
         LEFT JOIN dvapay.geo_country Country ON Country.id_country = Invoice.id_country
         LEFT JOIN dvapay.paysystems_external_fees PaysystemsExternalFees
                   ON (PaysystemsExternalFees.id_geotype = Invoice.id_geotype AND
                       PaysystemsExternalFees.id_project = Invoice.id_project)
         LEFT JOIN dvapay.paysystems_external_fees PaysystemsExternalFeesAnyProject
                   ON (PaysystemsExternalFeesAnyProject.id_geotype = Invoice.id_geotype AND
                       PaysystemsExternalFeesAnyProject.id_project = -1)
         LEFT JOIN dvapay.valuta_history CurrencyPaymentHistory
                   ON (CurrencyPaymentHistory.id = Invoice.currency_payment AND
                       CurrencyPaymentHistory.date = DATE(Invoice.date_payout))
         LEFT JOIN dvapay.valuta_history CurrencyDraftHistory
                   ON (CurrencyDraftHistory.id = IFNULL(Draft.currency_id, IF(UnionInvoice.Coef = -1 AND
                                                                              CanceledInvoice.rate IS NOT NULL AND
                                                                              CanceledInvoice.currency_draft IS NOT NULL AND
                                                                              CanceledInvoice.currency_draft !=
                                                                              Invoice.currency_draft,
                                                                              CanceledInvoice.currency_draft,
                                                                              IFNULL(Invoice.currency_draft, Agreement.id_currency_draft))) AND
                       CurrencyDraftHistory.date = DATE(Invoice.date_payout))
         LEFT JOIN dvapay.ps_hold_invoice PsHoldInvoice ON PsHoldInvoice.id_invoice = Invoice.id
         LEFT JOIN dvapay.refer_source Refer ON Refer.id = Invoice.id_refer
         LEFT JOIN  PsInstanceClass ON Invoice.id_instance = PsInstanceClass.IdInstance
         LEFT JOIN billing.invoice_gift_for_friend_details InvoiceGiftForFriendDetails
                   ON InvoiceGiftForFriendDetails.idInvoice = Invoice.id
         LEFT JOIN billing.invoice_parent InvoiceParent ON InvoiceParent.invoice_id = Invoice.id
         LEFT JOIN  InvoiceDirectTaxes ON InvoiceDirectTaxes.invoice_id = Invoice.id
         LEFT JOIN dvapay.valuta UserWalletCurrency ON UserWalletCurrency.id = InvoiceFinance.userWalletCurrencyId
         LEFT JOIN finance.accounts FinanceAccount ON FinanceAccount.id = Draft.finance_account_id
         LEFT JOIN dvapay.valuta CurrencyPayout ON CurrencyPayout.id = FinanceAccount.id_currency
         LEFT JOIN billing.parameter_values BillingParameterValues
                   ON BillingParameterValues.id = BillingRefundCases.id_parameter_value
         LEFT JOIN dvapay.valuta CurrencyProject ON CurrencyProject.id = InvoiceInfo.project_currency
         LEFT JOIN dvapay.valuta CurrencyProjectNominal
                   ON CurrencyProjectNominal.id = InvoiceInfo.currency_project_nominala