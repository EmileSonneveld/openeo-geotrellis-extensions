package org.openeo.geotrellis.icor;

import java.time.ZonedDateTime;

// Applies MODTRAN atmospheric correction based on preset values in a lookup table.
public class Landsat8Descriptor extends ICorCorrectionDescriptor{


	public Landsat8Descriptor() throws Exception {
		super();
	}

	public String getLookupTableURL() {
		return "https://artifactory.vgt.vito.be/artifactory/auxdata-public/lut/L8_big_disort.bin"; 
				//"L8_big_disort"; 
	}

    @Override
    public int getBandFromName(String name) throws IllegalArgumentException {
		switch(name.toUpperCase()) {
			case "B01":         return 0;
			case "B02":         return 1;
			case "B03":         return 2;
			case "B04":         return 3;
			case "B05":         return 4;
			case "B06":         return 5;
			case "B07":         return 6;
			case "B08":         return 7;
			case "B09":         return 8;
			case "B10":         return 9;
			case "B11":         return 10;
			default: throw new IllegalArgumentException("Unsupported band: "+name);
		}
	}
    
    // source:
    // official: http://www.gisagmaps.com/landsat-8-sentinel-2-bands/
    // B08 (from Thuillier spectrum): https://bleutner.github.io/RStoolbox/r/2016/01/26/estimating-landsat-8-esun-values
    // TODO: B10 and B11 has no values. Should be excluded from correction or extrap from Thuillier?
    final static double[] irradiances = {
	    1857.00, // B01
	    2067.00, // B02
	    1893.00, // B03
	    1603.00, // B04
	     972.60, // B05
	     245.00, // B06
	      79.72, // B07
	    1723.88, // B08
	     399.70, // B09
	 Double.NaN, // B10
	 Double.NaN  // B11
    };

    // source:
    // http://www.gisagmaps.com/landsat-8-sentinel-2-bands/
    // 
    final static double[] central_wavelengths = {
         442.96, // B01
         482.04, // B02
         561.41, // B03
         654.59, // B04
         864.67, // B05
        1608.86, // B06
        2200.73, // B07
         589.50, // B08
        1373.43, // B09
       10895.00, // B10
       12005.00  // B11
    };
/*
	// it is easier to use reflectance and call refl2rad because refl scaling is constant
    final static double[] RADIANCE_ADD_BAND = {
       -62.86466,
       -64.10541,
       -58.69893,
       -49.71440,
       -30.16725,
        -7.60064,
        -2.47247,
       -56.00013,
       -12.39698
    };

    final static double[] RADIANCE_MULT_BAND = {
        1.2573E-02,
        1.2821E-02,
        1.1740E-02,
        9.9429E-03,
        6.0335E-03,
        1.5201E-03,
        4.9449E-04,
        1.1200E-02,
        2.4794E-03
    };
    */
    @Override
	public double getIrradiance(int iband) {
		return irradiances[iband];
	}

    @Override
	public double getCentralWavelength(int iband) {
		return central_wavelengths[iband];
	}

	/**
     * @param src: digital number of the top of the atmosphere TOA radiance 
     * @return TOA reflectance  (float 0..1)
	 */
	@Override
	public double preScale(double src, double sza, ZonedDateTime time, int bandIdx) {
		// lut only has 8 bands instead of 9
		if (bandIdx>7) return src;
        // TODO: this works with sentinelhub's landsat8 layer only because they changed scaling and also divide by cos(sza) compared to stock landsat8. For CreoDias they provide L8 without processing
//		return reflToRad(src*0.0001, sza, getIrradiance(bandIdx));
		return reflToRad_with_earthsundistance(src*0.0001, sza, time, getIrradiance(bandIdx));
	}
    
	/**
     * @param src: TOA radiance
     * @return: BOA reflectance in digital number
	 */
	@Override
    public double correct(
    		String bandName,
    		int bandIdx,
    		ZonedDateTime time,
    		double src, 
    		double sza, 
    		double vza, 
    		double raa, 
    		double gnd, 
    		double aot, 
    		double cwv, 
    		double ozone,
    		int waterMask)
    {
		// lut only has 8 bands instead of 9
		if (bandIdx>7) return src;
/*
		final double TOAradiance=src*RADIANCE_MULT_BAND[band]+RADIANCE_ADD_BAND[band];
        final double corrected = correctRadiance( band, TOAradiance, sza, vza, raa, gnd, aot, cwv, ozone, waterMask);
		//final double corrected=TOAradiance;
        return corrected*10000.;
*/
        // Apply atmoshperic correction on pixel based on an array of parameters from MODTRAN
        final double corrected = correctRadiance( bandIdx, src, sza, vza, raa, gnd, aot, cwv, ozone, waterMask);
		//final double corrected=TOAradiance;
        return corrected*10000.;    
    }

	
	
}
