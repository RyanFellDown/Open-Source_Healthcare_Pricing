from geopy.geocoders import Nominatim

def getCoordinates():
    address_parts = [provider.street, provider.city, provider.state, provider.zip]
    address = ", ".join(filter(None, address_parts))
    location = geolocator.geocode(address)
    if location:
        return {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "provider": provider.dict()
        }
    else:
        # Fallback: just ZIP centroid
        zip_location = geolocator.geocode(provider.zip)
        if zip_location:
            return {
                "latitude": zip_location.latitude,
                "longitude": zip_location.longitude,
                "provider": provider.dict()
            }
    return {"error": "Unable to geocode"}