from protocolo.config import USGS_USERNAME, USGS_PASSWORD, EMAIL_RECIPIENTS, validate_config

print('USGS User:', USGS_USERNAME)
print('Email recipients:', len(EMAIL_RECIPIENTS))
print('Config loaded successfully!')

# Validate critical variables are set
validate_config()
print('All critical variables validated!')

