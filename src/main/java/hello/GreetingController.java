package hello;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import com.isl.upstart.ucrawlex.common.constants.ExUCrawlCommonConstants;
import com.isl.upstart.ucrawlex.common.dtos.mapmonitor.MapMonitorJobDTO;
import hello.services.FileUploadService;

@RestController
public class GreetingController {

  @Autowired
  FileUploadService fileUploadService;

  private static final String template = "Hello, %s!";
  private final AtomicLong counter = new AtomicLong();

  @CrossOrigin(origins = "*")
  @GetMapping("/greeting")
  public Greeting greeting(
      @RequestParam(required = false, defaultValue = "World") final String name) {
    System.out.println("==== in greeting ====");
    return new Greeting(counter.incrementAndGet(), String.format(template, name));
  }

  @GetMapping("/greeting-javaconfig")
  public Greeting greetingWithJavaconfig(
      @RequestParam(required = false, defaultValue = "World") final String name) {
    System.out.println("==== in greeting ====");
    return new Greeting(counter.incrementAndGet(), String.format(template, name));
  }

  @PostMapping("/mapmonitorjobssample")
  public Greeting handleFileUpload(
      @RequestParam(required = false, defaultValue = "World") final String name) {
    System.out.println("==== in POST HANDLE ====");
    return new Greeting(counter.incrementAndGet(), String.format(template, name));
  }

  @PostMapping("/mapmonitorjobs")
  public ResponseEntity<Object> handleExcelFileUpload(
      @RequestPart(ExUCrawlCommonConstants.FORM_FIELD_FILE) final MultipartFile uploadedFile,
      @RequestPart(ExUCrawlCommonConstants.FORM_FIELD_JOBTITLE) final String jobName,
      @RequestPart("token") final String token) throws Exception {

    if (StringUtils.isBlank(jobName) || StringUtils.isBlank(token) || null == uploadedFile
        || uploadedFile.isEmpty())
      return new ResponseEntity<>("Required form fields are missing...", HttpStatus.BAD_REQUEST);

    System.out.println("Job Name" + jobName);
    System.out.println("Uploaded file " + uploadedFile.getOriginalFilename());

    final MapMonitorJobDTO resp = fileUploadService.createJob(jobName, uploadedFile, token);

    if (null != resp) {
      final CreateResponseDTO dto = new CreateResponseDTO();
      dto.setCandidateSize(resp.getCandidateSize());
      dto.setCreatedAt(resp.getCreatedAt());
      dto.setCreatedBy(resp.getCreatedBy());
      dto.setId(resp.getId());
      dto.setStatus(resp.getStatus());
      dto.setTitle(resp.getTitle());
      dto.setVendor(resp.getVendor());

      return new ResponseEntity<>(dto, HttpStatus.CREATED);
    } else
      return new ResponseEntity<>("failed to create job.. try again ",
          HttpStatus.PRECONDITION_FAILED);

  }

}
