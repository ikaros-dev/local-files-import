package run.ikaros.plugin.file;

import org.pf4j.PluginWrapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.ikaros.api.constant.FileConst;
import run.ikaros.api.core.attachment.Attachment;
import run.ikaros.api.core.attachment.AttachmentOperate;
import run.ikaros.api.infra.properties.IkarosProperties;
import run.ikaros.api.infra.utils.FileUtils;
import run.ikaros.api.plugin.BasePlugin;
import run.ikaros.api.store.enums.AttachmentType;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LocalFilesImportPlugin extends BasePlugin {
    private static final String NAME = "PluginLocalFilesImport";
    private final IkarosProperties ikarosProperties;
    private final AttachmentOperate attachmentOperate;

    public LocalFilesImportPlugin(PluginWrapper wrapper, IkarosProperties ikarosProperties,
                                  AttachmentOperate attachmentOperate) {
        super(wrapper);
        this.ikarosProperties = ikarosProperties;
        this.attachmentOperate = attachmentOperate;
    }


    @Override
    public void start() {
        log.info("plugin [{}] start success", NAME);
        linkFiles();
    }

    @Override
    public void stop() {
        log.info("plugin [{}] stop success", NAME);
    }

    @Override
    public void delete() {
        log.debug("plugin [{}] delete success", NAME);
    }


    @Async
    protected void linkFiles() {
        log.info("start import links dir files.");
        long start = System.currentTimeMillis();
        Path links = ikarosProperties.getWorkDir().resolve(FileConst.DEFAULT_IMPORT_DIR_NAME);
        File linksDir = links.toFile();
        if (linksDir.isFile()) {
            throw new LocalFilesImportException("current links must dir.");
        }
        if (!linksDir.exists()) {
            linksDir.mkdirs();
            log.info("mkdir links dir, path={}", linksDir);
        }

        handleImportDirFile(linksDir, 0L)
            .subscribeOn(Schedulers.parallel())
            .subscribe();

        log.info("end import links dir files, time: {}", System.currentTimeMillis() - start);
    }


    private Mono<Void> handleImportDirFile(File file, Long parentId) {
        Assert.notNull(file, "'file' must not null");
        Assert.isTrue(parentId >= 0, "'parentId' must >= 0");
        log.debug("current file: {}", file.getAbsolutePath());
        log.debug("current parentId: {}", parentId);
        if (file.isDirectory()) {
            // ignore . and .. create dir.
            if (".".equalsIgnoreCase(file.getName()) || "..".equalsIgnoreCase(file.getName())) {
                return Mono.empty();
            }
            // dir
            return attachmentOperate.findByTypeAndParentIdAndName(
                    AttachmentType.Directory, parentId, file.getName())
                .doOnSuccess(attachment ->
                    log.debug("{} dir attachment for logic path[{}] "
                            + "by find by type[Directory] and parentId[{}]",
                        Objects.isNull(attachment) ? "NotExists" : "Exists",
                        Objects.isNull(attachment) ? null : attachment.getPath(), parentId))
                .switchIfEmpty(attachmentOperate.createDirectory(parentId, file.getName())
                    .checkpoint("create directory for parentId=" + parentId
                        + " and file=" + file.getAbsolutePath())
                    .doOnSuccess(attachment ->
                        log.debug("Create dir attachment[{}].", attachment.getPath())))
                .flatMapMany(attachment -> Flux.fromArray(Objects.requireNonNull(file.listFiles()))
                    .flatMap(file1 -> handleImportDirFile(file1, attachment.getId())
                        .checkpoint("call handleImportDirFile by "
                            + "parentId=" + attachment.getId() + " and file=" +
                            file1.getAbsolutePath())))
                .then();
        } else {
            // file
            if (!file.exists()) {
                return Mono.empty();
            }

            // 如果数据库对应目录存在对应附件，也直接返回
            return attachmentOperate.findByTypeAndParentIdAndName(AttachmentType.File,
                    parentId, file.getName())
                .map(attachment -> {
                    log.debug("Skip linkAndSaveAttachment operate for file attachment exists, path[{}].",
                        attachment.getPath());
                    return attachment;
                })
                .switchIfEmpty(linkAndSaveAttachment(file, parentId))
                .then();
        }
    }

    private Mono<Attachment> linkAndSaveAttachment(File file, Long parentId) {
        // 创建上传文件目录
        String name = file.getName();
        String filePostfix = FileUtils.parseFilePostfix(name);
        String uploadFilePath
            = FileUtils.buildAppUploadFilePath(
            ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
            filePostfix);
        File uploadFile = new File(uploadFilePath);

        return Mono.just(uploadFile)
            .publishOn(Schedulers.boundedElastic())
            .map(file1 -> {
                log.debug("Attachment not exists, will link and save for parentId[{}] "
                    + "and exists import file[{}]", parentId, file.getAbsolutePath());
                try {
                    Files.createLink(uploadFile.toPath(), file.toPath());
                    log.info("copy file hard link from: {}, to: {}",
                        file.getAbsolutePath(), uploadFile.getAbsolutePath());
                    return file1;
                } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                    log.warn("file already exists for path: [{}].", uploadFile.getAbsolutePath());
                    return file1;
                } catch (IOException fileSystemException) {
                    // 硬链接失败则进行复制
                    log.warn("file hard links fail from: {}, to: {}",
                        file.getAbsolutePath(), uploadFile.getAbsolutePath());
                    try {
                        Files.copy(file.toPath(), uploadFile.toPath());
                        log.info("copy file from: {}, to: {}",
                            file.getAbsolutePath(), uploadFile.getAbsolutePath());
                        return file1;
                    } catch (IOException e) {
                        log.error("skip operate, copy file fail from: {}, to: {}"
                            , file.getAbsolutePath(), uploadFile.getAbsolutePath()
                            , e);
                        throw new RuntimeException(e);
                    }
                }
            })
            .map(f -> Attachment.builder()
                .parentId(parentId).name(name).type(AttachmentType.File)
                .fsPath(uploadFile.getAbsolutePath()).size(uploadFile.length())
                .updateTime(LocalDateTime.now())
                .url(uploadFilePath
                    .replace(ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
                        "")
                    .replace("\\", "/")).build())
            .flatMap(f -> attachmentOperate.save(f)
                .doOnSuccess(atta -> log.debug("Success link and save single attachment[{}].",
                    atta.getPath())));
    }
}
