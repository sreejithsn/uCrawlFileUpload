package hello.services;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.javafunk.excelparser.SheetParser;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import com.google.gson.Gson;
import com.isl.upstart.ucrawlex.common.constants.JobStatus;
import com.isl.upstart.ucrawlex.common.dao.MapMonitorJobDAO;
import com.isl.upstart.ucrawlex.common.dao.MapMonitorJobRunDAO;
import com.isl.upstart.ucrawlex.common.dao.MapMonitorJobTargetDAO;
import com.isl.upstart.ucrawlex.common.dao.impl.MapMonitorJobDAOImpl;
import com.isl.upstart.ucrawlex.common.dao.impl.MapMonitorJobRunDAOImpl;
import com.isl.upstart.ucrawlex.common.dao.impl.MapMonitorJobTargetDAOImpl;
import com.isl.upstart.ucrawlex.common.datum.MapInputData;
import com.isl.upstart.ucrawlex.common.datum.MapJobAsinFileInput;
import com.isl.upstart.ucrawlex.common.datum.TokenUserInfo;
import com.isl.upstart.ucrawlex.common.dtos.VendorDTO;
import com.isl.upstart.ucrawlex.common.dtos.mapmonitor.MapJobCandidateDTO;
import com.isl.upstart.ucrawlex.common.dtos.mapmonitor.MapMonitorJobDTO;
import com.isl.upstart.ucrawlex.common.dtos.mapmonitor.MapMonitorUploadDTO;
import com.isl.upstart.ucrawlex.common.exceptions.ExtendedUcrawlException;
import com.isl.upstart.ucrawlex.common.exceptions.InvalidInputDataException;
import com.isl.upstart.ucrawlex.common.exceptions.UnauthorizedAccessException;
import com.isl.upstart.ucrawlex.common.models.MapMonitorJobEntry;
import com.isl.upstart.ucrawlex.common.models.MapMonitorJobTargetEntry;
import com.isl.upstart.ucrawlex.common.utils.ExUcrawlCommonUtil;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FileUploadService {

  private boolean hasParseErrors = false;

  public List<MapInputData> getSellerInputListFromFile(final MultipartFile uploadedFile)
      throws ExtendedUcrawlException {

    try {
      final InputStream inputFile = uploadedFile.getInputStream();

      final List<MapJobAsinFileInput> rawInputList = new SheetParser().createEntity(
          new XSSFWorkbook(inputFile).getSheetAt(0), MapJobAsinFileInput.class, parseEx -> {
            hasParseErrors = true;
            log.warn("Exception in parsing excel data: [{}].", parseEx);
          });

      if (hasParseErrors)
        throw new InvalidInputDataException(
            "Failed to parse some entries in the input file. Please try again.");

      if (CollectionUtils.isEmpty(rawInputList))
        throw new InvalidInputDataException("There is no ASINs/URLs to process.");

      final List<MapInputData> sellerInputList =
          validateEntriesAndBuildSellerInputData(rawInputList);

      if (CollectionUtils.isEmpty(sellerInputList))
        throw new InvalidInputDataException("There is no ASINs/URLs to process.");

      return sellerInputList;

    } catch (final IOException e) {
      log.error("Failed to process the input file", e);
      throw new ExtendedUcrawlException("Failed process the input file. Please try again.");
    }

  }

  private List<MapInputData> validateEntriesAndBuildSellerInputData(
      final List<MapJobAsinFileInput> rawDataRecords) throws InvalidInputDataException {

    final List<MapInputData> sellerpageUrlsList = new ArrayList<>();

    for (final MapJobAsinFileInput rawData : rawDataRecords) {
      if (StringUtils.isNotBlank(rawData.getAsin())) {

        if (StringUtils.length(rawData.getAsin().trim()) != 10)
          throw new InvalidInputDataException("Invalid ASIN specified in the input list.");

        sellerpageUrlsList
            .add(new MapInputData(rawData.getAsin(), rawData.getMap(), rawData.getTargetEmails()));

      }
    }
    return sellerpageUrlsList;
  }

  private final Gson gson = new Gson();

  private final MapMonitorJobDAO jobDao = new MapMonitorJobDAOImpl();
  private final MapMonitorJobTargetDAO targetDao = new MapMonitorJobTargetDAOImpl();
  private final MapMonitorJobRunDAO runDao = new MapMonitorJobRunDAOImpl();


  public MapMonitorJobDTO createNewMapMonitorJob(final MapMonitorUploadDTO jobData,
      final TokenUserInfo requestingUser) throws ExtendedUcrawlException {
    validateUserInfoInRequest(requestingUser);

    final MapMonitorJobEntry createdEntry =
        jobDao.createMapMonitorJob(buildModelEntryFromDto(jobData, requestingUser));

    if (null == createdEntry) {
      log.error("Failed to create the Keyword relevance Job Entry- DB insert returns NULL");
      throw new ExtendedUcrawlException("Failed to create job entry");
    }

    saveCandidatesEntryForJob(createdEntry, jobData.getMapInput(), requestingUser);

    return createKeywordRelevanceDtoFromModel(createdEntry, null)
        .orElseThrow(() -> new ExtendedUcrawlException("Failed to create Response object."));

  }

  private void validateUserInfoInRequest(final TokenUserInfo requestingUser)
      throws UnauthorizedAccessException {
    if (null == requestingUser) {
      log.info("Failed to get User Info from request Header.");
      throw new UnauthorizedAccessException("Unauthorized.. No user data available in request.");
    }
  }

  private MapMonitorJobEntry buildModelEntryFromDto(final MapMonitorUploadDTO jobData,
      final TokenUserInfo requestingUser) {

    final MapMonitorJobEntry entry = new MapMonitorJobEntry();
    entry.setCandidateSize(jobData.getMapInput().size());
    entry.setJobTitle(jobData.getJobTitle());

    entry.setStatus(JobStatus.READY);

    entry.setCreatedVendorName(requestingUser.getVendorName());
    entry.setCreatedByUserName(StringUtils.joinWith(StringUtils.SPACE,
        requestingUser.getFirstName(), requestingUser.getLastName()));
    entry.setCreatedBy(requestingUser.getUserId());
    entry.setUpdatedBy(requestingUser.getUserId());

    final Instant now = Instant.now();
    entry.setCreatedAt(now.getEpochSecond());
    entry.setUpdatedAt(now.getEpochSecond());
    return entry;

  }

  private void saveCandidatesEntryForJob(final MapMonitorJobEntry createdEntry,
      final List<MapInputData> mapInput, final TokenUserInfo requestingUser)
      throws ExtendedUcrawlException {

    final List<MapMonitorJobTargetEntry> targetsToSave = new ArrayList<>();

    mapInput.forEach(item -> {
      final MapMonitorJobTargetEntry entry = new MapMonitorJobTargetEntry();
      entry.setAsin(item.getInput());
      entry.setCreatedAt(Instant.now().getEpochSecond());
      entry.setCreatedBy(requestingUser.getUserId());
      entry.setJobId(createdEntry.getId());
      entry.setMap(item.getMap());

      if (StringUtils.isNotBlank(item.getEmailTarget())) {
        final List<String> emailList = Arrays.asList(StringUtils.split(item.getEmailTarget(), ","));
        entry.setNotificationEmails(emailList);
      }

      targetsToSave.add(entry);
    });

    if (CollectionUtils.isNotEmpty(targetsToSave)) {
      targetDao.saveMapMonitorJobTargets(targetsToSave);
    }

  }

  private Optional<MapMonitorJobDTO> createKeywordRelevanceDtoFromModel(
      final MapMonitorJobEntry model, final List<MapMonitorJobTargetEntry> candidates) {
    if (null == model)
      return Optional.empty();

    final MapMonitorJobDTO dto = new MapMonitorJobDTO();
    dto.setId(model.getId().toHexString());
    dto.setCreatedAt(model.getCreatedAt());
    dto.setCreatedBy(model.getCreatedByUserName());
    dto.setLastRunAt(model.getLastRunAt());

    dto.setStatus(model.getStatus().getDisplayName());
    dto.setTitle(model.getJobTitle());
    dto.setCandidateSize(model.getCandidateSize());

    final VendorDTO vendor = new VendorDTO();
    vendor.setId(model.getCreatedBy());
    vendor.setName(model.getCreatedVendorName());
    dto.setVendor(vendor);

    if (CollectionUtils.isNotEmpty(candidates)) {
      dto.setCandidates(buildCandidatesDto(candidates));
    }

    return Optional.of(dto);
  }


  private List<MapJobCandidateDTO> buildCandidatesDto(
      final List<MapMonitorJobTargetEntry> candidates) {
    final List<MapJobCandidateDTO> candidateList = new ArrayList<>();

    candidates.forEach(item -> {
      final MapJobCandidateDTO candidate = new MapJobCandidateDTO();
      candidate.setAsin(item.getAsin());
      candidate.setEmails(item.getNotificationEmails());
      candidate.setId(item.getId().toHexString());

      if (null != item.getMap()) {
        candidate.setMap("$" + item.getMap());
      }

      candidateList.add(candidate);

    });

    return candidateList;
  }

  public MapMonitorJobDTO createJob(final String jobName, final MultipartFile uploadedFile,
      final String token) throws ExtendedUcrawlException {
    final MapMonitorUploadDTO dto = new MapMonitorUploadDTO();
    dto.setJobTitle(jobName);
    dto.setMapInput(getSellerInputListFromFile(uploadedFile));

    final Optional<TokenUserInfo> requestingUser = ExUcrawlCommonUtil.buildUserInfoFromJWT(token);

    return createNewMapMonitorJob(dto, requestingUser.get());

  }



}
