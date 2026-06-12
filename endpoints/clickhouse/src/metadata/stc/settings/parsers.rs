use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;

struct SettingCommon {
    name: String,
    current_value: String,
    default_value: String,
    description: Option<String>,
    is_readonly: bool,
}

fn parse_setting_common(row: &ClickhouseRow) -> ResultEP<SettingCommon> {
    Ok(SettingCommon {
        name: row.required_string("name")?,
        current_value: row.required_string("value")?,
        default_value: row.required_string("default_value")?,
        description: row.optional_string("description")?,
        is_readonly: row.required_bool("readonly")?,
    })
}

pub(super) fn parse_inconsistent_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseInconsistentSetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        settings.push(ClickhouseInconsistentSetting {
            name: row.required_string("name")?,
            values: row.required_string("values")?,
            hosts: row.required_string("hosts")?,
            impact_level: SettingImpactLevel::Medium,
        });
    }

    Ok(settings)
}

pub(super) fn parse_deprecated_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDeprecatedSetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        let common = parse_setting_common(&row)?;
        settings.push(ClickhouseDeprecatedSetting {
            replacement_setting: ClickhouseSettingsInfo::get_replacement_setting(&common.name),
            deprecation_reason: ClickhouseSettingsInfo::get_deprecation_reason(&common.name),
            name: common.name,
            current_value: common.current_value,
            default_value: common.default_value,
            description: common.description,
            is_readonly: common.is_readonly,
        });
    }

    Ok(settings)
}

pub(super) fn parse_memory_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMemorySetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        let common = parse_setting_common(&row)?;
        let name = common.name.clone();
        let current_value = common.current_value.clone();

        settings.push(ClickhouseMemorySetting {
            name: name.clone(),
            current_value: current_value.clone(),
            default_value: common.default_value,
            description: common.description,
            memory_impact: ClickhouseSettingsInfo::calculate_memory_impact(&name, &current_value),
            recommended_value: ClickhouseSettingsInfo::get_recommended_memory_value(&name),
            is_readonly: common.is_readonly,
        });
    }

    Ok(settings)
}

pub(super) fn parse_performance_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePerformanceSetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        let common = parse_setting_common(&row)?;
        let name = common.name.clone();
        let current_value = common.current_value.clone();

        settings.push(ClickhousePerformanceSetting {
            name: name.clone(),
            current_value: current_value.clone(),
            default_value: common.default_value,
            description: common.description,
            performance_impact: ClickhouseSettingsInfo::calculate_performance_impact(&name, &current_value),
            recommended_value: ClickhouseSettingsInfo::get_recommended_performance_value(&name),
            is_readonly: common.is_readonly,
        });
    }

    Ok(settings)
}

pub(super) fn parse_security_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseSecuritySetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        let common = parse_setting_common(&row)?;
        let name = common.name.clone();
        let current_value = common.current_value.clone();

        settings.push(ClickhouseSecuritySetting {
            name: name.clone(),
            current_value: current_value.clone(),
            default_value: common.default_value,
            description: common.description,
            security_level: ClickhouseSettingsInfo::calculate_security_level(&name, &current_value),
            recommended_value: ClickhouseSettingsInfo::get_recommended_security_value(&name),
            is_readonly: common.is_readonly,
        });
    }

    Ok(settings)
}

pub(super) fn parse_resource_limit_settings(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseResourceLimitSetting>> {
    let mut settings = Vec::with_capacity(rows.len());

    for row in rows {
        let common = parse_setting_common(&row)?;
        let name = common.name.clone();
        let current_value = common.current_value.clone();

        settings.push(ClickhouseResourceLimitSetting {
            name: name.clone(),
            current_value: current_value.clone(),
            default_value: common.default_value,
            description: common.description,
            resource_type: ClickhouseSettingsInfo::determine_resource_type(&name),
            limit_impact: ClickhouseSettingsInfo::calculate_limit_impact(&name, &current_value),
            recommended_value: ClickhouseSettingsInfo::get_recommended_limit_value(&name),
            is_readonly: common.is_readonly,
        });
    }

    Ok(settings)
}
