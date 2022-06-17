use crate::prelude::FeatureFlag;
use lazy_static::lazy_static;
use semver::{Version, VersionReq};
use std::collections::HashMap;

lazy_static! {
    pub static ref VERSIONS: HashMap<Version, Vec<FeatureFlag>> = {
        let mut supported_versions: Vec<(Version, Vec<FeatureFlag>)> = vec![
            // baseline version
            (Version::new(1, 0, 0), vec![]),
            // Versions with feature flags
            (Version::new(1, 1, 0), vec![FeatureFlag::BasicOrdering])
        ];

        let mut map = HashMap::new();

        // Sort version by major, minor, patch, from higher to lower.
        supported_versions.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        for (version, flags) in supported_versions {
            map.insert(version, flags);
        }

        map
    };

    static ref LATEST_VERSION: String = {
        let keys: Vec<Version> = VERSIONS.clone().into_keys().collect();

        // Versions are sorted by major, minor, patch, from higher to lower.
        keys.first().unwrap().to_string()
    };
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ApiVersion {
    pub version: Version,
    features: Vec<FeatureFlag>,
}

impl ApiVersion {
    pub fn new(version_requirement: &VersionReq) -> Result<Self, String> {
        let version = Self::resolve(&version_requirement);

        if version.is_none() {
            return Err("No versions found".to_string());
        }

        match version {
            Some(version) => Ok(Self {
                version: version.clone(),
                features: VERSIONS
                    .get(&version)
                    .expect(format!("Version {:?} is not supported", version).as_str())
                    .to_vec(),
            }),
            None => Err("No versions found".to_string()),
        }
    }

    pub fn supports(&self, feature: FeatureFlag) -> bool {
        self.features.contains(&feature)
    }

    fn resolve(version_requirement: &VersionReq) -> Option<Version> {
        for (version, _) in VERSIONS.iter() {
            if version_requirement.matches(version) {
                return Some(version.clone());
            }
        }

        None
    }
}

impl Default for ApiVersion {
    fn default() -> Self {
        ApiVersion::new(&VersionReq::default()).unwrap()
    }
}
