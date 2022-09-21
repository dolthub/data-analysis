
/*

	Here's the preliminary Postgresql SQL Schema.
	I'm positive that there are some errors, misspellings at least. But the basics of the schema design checks out.
	This version is the first to use UUID's for table primary keys.
	
	
	I still have a few design/domain questions

	What is a Provider_Group exactly? Here I'm referring to a single one as a ProviderElement. Is it like a clinic in a hospital?
	
	Are there any joins or links here that should be identifying relationships? I've left things loosely coupled on purpose.
	
	Is ServiceCode an array or list?
	
	Is BillingCodeModifier an array or list?
	
	
	
*/



CREATE TABLE HealthInsurance.NegotiatedPrices (
                NegotiatedPricesID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                NegotiatedRateValue NUMERIC(3,12) DEFAULT 0 NOT NULL,
                ServiceCode VARCHAR NOT NULL,
                NegotiatedType VARCHAR NOT NULL,
                ExpirationDate DATE NOT NULL,
                BillingClass VARCHAR NOT NULL,
                BillingCodeModifier VARCHAR NOT NULL,
                AdditionalInformation VARCHAR NOT NULL,
                CONSTRAINT negotiatedpricesid_pk PRIMARY KEY (NegotiatedPricesID)
);
COMMENT ON COLUMN HealthInsurance.NegotiatedPrices.NegotiatedRateValue IS 'In the spec as "negotiated_rate"';
COMMENT ON COLUMN HealthInsurance.NegotiatedPrices.ServiceCode IS 'May be an array?';
COMMENT ON COLUMN HealthInsurance.NegotiatedPrices.BillingCodeModifier IS 'Most likely a list, need to ask';



CREATE TABLE HealthInsurance.NPIList (
                NPIListID BIGINT NOT NULL,
                NPIValue VARCHAR NOT NULL,
                CONSTRAINT npilistid_pk PRIMARY KEY (NPIListID)
);
COMMENT ON TABLE HealthInsurance.NPIList IS 'List of NPI''s for each Provider Element';




CREATE TABLE HealthInsurance.ProviderElement (
                ID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                TINType VARCHAR DEFAULT ein NOT NULL,
                TINValue VARCHAR NOT NULL,
                CONSTRAINT providerelementid_pk PRIMARY KEY (ID)
);
COMMENT ON TABLE HealthInsurance.ProviderElement IS 'Maps to an individual Provider element in the Provider_Groups object';
COMMENT ON COLUMN HealthInsurance.ProviderElement.ID IS 'Defined outside of CMS spec';




CREATE TABLE HealthInsurance.ProviderElement_NPIList (
                ProviderElement_NPIListID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                ID BIGINT NOT NULL,
                NPIListID BIGINT NOT NULL,
                CONSTRAINT providerelement_npilistid_pk PRIMARY KEY (ProviderElement_NPIListID)
);
COMMENT ON TABLE HealthInsurance.ProviderElement_NPIList IS 'Links the list of NPI''s to the Provider Element';
COMMENT ON COLUMN HealthInsurance.ProviderElement_NPIList.ProviderElement_NPIListID IS 'Internal Link table ID';
COMMENT ON COLUMN HealthInsurance.ProviderElement_NPIList.ID IS 'Defined outside of CMS spec';



CREATE TABLE HealthInsurance.ProviderGroup (
                ID BIGINT NOT NULL,
                ProviderGroupID BIGINT NOT NULL,
                ID_1 BIGINT NOT NULL,
                CONSTRAINT providergroupid_pk PRIMARY KEY (ID)
);
COMMENT ON TABLE HealthInsurance.ProviderGroup IS 'Link table for provider group lookup';
COMMENT ON COLUMN HealthInsurance.ProviderGroup.ID IS 'Internal ID for individual links,not in CMS spec';
COMMENT ON COLUMN HealthInsurance.ProviderGroup.ProviderGroupID IS 'The ID of the provider group, as defined in provider_group_id';
COMMENT ON COLUMN HealthInsurance.ProviderGroup.ID_1 IS 'Defined outside of CMS spec';


CREATE INDEX provider_group_idx
 ON HealthInsurance.ProviderGroup
 ( ProviderGroupID );

CLUSTER provider_group_idx ON Provider Group;



CREATE TABLE HealthInsurance.ReportingEntity (
                ID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                Name VARCHAR NOT NULL,
                EntityType VARCHAR NOT NULL,
                LastUpdated DATE NOT NULL,
                ExportVersion VARCHAR NOT NULL,
                CONSTRAINT reportingentityid_pk PRIMARY KEY (ID)
);
COMMENT ON COLUMN HealthInsurance.ReportingEntity.ID IS 'Defined outside of CMS spec';



CREATE TABLE HealthInsurance.ReportingEntity_ProviderGroup (
                ReportingEntity_ProviderGroupLinkID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                ID BIGINT NOT NULL,
                ID_1 INTEGER NOT NULL,
                CONSTRAINT reportingentity_providergrouplinkid_pk PRIMARY KEY (ReportingEntity_ProviderGroupLinkID)
);
COMMENT ON COLUMN HealthInsurance.ReportingEntity_ProviderGroup.ID IS 'Internal ID for individual links,not in CMS spec';
COMMENT ON COLUMN HealthInsurance.ReportingEntity_ProviderGroup.ID_1 IS 'Defined outside of CMS spec';



CREATE TABLE HealthInsurance.NegotiationArrangement (
                NegotiationArrangementID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                Name VARCHAR NOT NULL,
                BillingCodeType VARCHAR NOT NULL,
                Description VARCHAR NOT NULL,
                BillingCodeTypeVersion VARCHAR NOT NULL,
                BillingCode VARCHAR NOT NULL,
                ArrangementType VARCHAR NOT NULL,
                ID INTEGER NOT NULL,
                CONSTRAINT negotiationarrangementid_pk PRIMARY KEY (NegotiationArrangementID)
);
COMMENT ON TABLE HealthInsurance.NegotiationArrangement IS 'Internal ID, not defined by the CMS Spec';
COMMENT ON COLUMN HealthInsurance.NegotiationArrangement.ArrangementType IS 'Hlds the value given by negotiation_arrangement';
COMMENT ON COLUMN HealthInsurance.NegotiationArrangement.ID IS 'Defined outside of CMS spec';




CREATE TABLE HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table (
                NegotiationArrangement_NegotiatedRates_LinkID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                NegotiationArrangementID BIGINT NOT NULL,
                ID BIGINT NOT NULL,
                CONSTRAINT negotiationarrangement_negotiatedrates_linkid_pk PRIMARY KEY (NegotiationArrangement_NegotiatedRates_LinkID)
);
COMMENT ON TABLE HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table IS 'Known as "negotiated_rates" in the CMS spec';
COMMENT ON COLUMN HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table.ID IS 'Internal ID for individual links,not in CMS spec';




CREATE TABLE HealthInsurance.NegotiatedPriceGroup (
                NegotiatedPriceGroupID UUID NOT NULL DEFAULT uuid_generate_v1() , 
                NegotiatedPricesID BIGINT NOT NULL,
                NegotiationArrangement_NegotiatedRates_LinkID BIGINT NOT NULL,
                CONSTRAINT negotiatedpricegroupid_pk PRIMARY KEY (NegotiatedPriceGroupID)
);
COMMENT ON TABLE HealthInsurance.NegotiatedPriceGroup IS 'Grouping table for negotiated prices';



ALTER TABLE HealthInsurance.NegotiatedPriceGroup ADD CONSTRAINT negotiatedprices_negotiatedpricegroup_fk
FOREIGN KEY (NegotiatedPricesID)
REFERENCES HealthInsurance.NegotiatedPrices (NegotiatedPricesID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.ProviderElement_NPIList ADD CONSTRAINT npilist_providerelement_npilist_fk
FOREIGN KEY (NPIListID)
REFERENCES HealthInsurance.NPIList (NPIListID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.ProviderGroup ADD CONSTRAINT providerelement_providergroup_fk
FOREIGN KEY (ID_1)
REFERENCES HealthInsurance.ProviderElement (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.ProviderElement_NPIList ADD CONSTRAINT providerelement_providerelement_npilist_fk
FOREIGN KEY (ID)
REFERENCES HealthInsurance.ProviderElement (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.ReportingEntity_ProviderGroup ADD CONSTRAINT providergroup_reportingentity_providergroup_fk
FOREIGN KEY (ID)
REFERENCES HealthInsurance.ProviderGroup (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table ADD CONSTRAINT providergroup_negotiation_arrangement_negotiated_rates_linki382
FOREIGN KEY (ID)
REFERENCES HealthInsurance.ProviderGroup (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.NegotiationArrangement ADD CONSTRAINT reportingentity_negotiationarrangement_fk
FOREIGN KEY (ID)
REFERENCES HealthInsurance.ReportingEntity (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.ReportingEntity_ProviderGroup ADD CONSTRAINT reportingentity_reportingentity_providergroup_fk
FOREIGN KEY (ID_1)
REFERENCES HealthInsurance.ReportingEntity (ID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table ADD CONSTRAINT negotiationarrangement_negotiation_arrangement_negotiated_ra625
FOREIGN KEY (NegotiationArrangementID)
REFERENCES HealthInsurance.NegotiationArrangement (NegotiationArrangementID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;

ALTER TABLE HealthInsurance.NegotiatedPriceGroup ADD CONSTRAINT negotiation_arrangement_negotiated_rates_linking_table_negot414
FOREIGN KEY (NegotiationArrangement_NegotiatedRates_LinkID)
REFERENCES HealthInsurance.Negotiation_Arrangement_Negotiated_Rates_Linking_Table (NegotiationArrangement_NegotiatedRates_LinkID)
ON DELETE NO ACTION
ON UPDATE NO ACTION
NOT DEFERRABLE;
